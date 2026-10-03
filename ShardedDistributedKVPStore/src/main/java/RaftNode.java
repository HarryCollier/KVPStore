import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode {
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Address selfAddress;
    private final SimpleKVPStore store;
    private final NodeConnectionPoolManager connectionPoolManager;

    private volatile Role state = Role.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile Address votedFor = null;
    private volatile Address currentLeader = null;

    private final List<RaftLogEntry> log = Collections.synchronizedList(new ArrayList<>());
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private volatile Shard shard;
    private int votesReceived = 0;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService asyncRpcExecutor = Executors.newCachedThreadPool();
    private ScheduledFuture<?> electionTimerFuture;
    private ScheduledFuture<?> heartbeatTimerFuture;
    private final Random random = new Random();

    public RaftNode(Address selfAddress, SimpleKVPStore store, NodeConnectionPoolManager connectionPoolManager) {
        this.selfAddress = selfAddress;
        this.store = store;
        this.connectionPoolManager = connectionPoolManager;
    }

    public synchronized void updateShard(Shard newShard) {
        this.shard = newShard;
        if (electionTimerFuture == null && state == Role.FOLLOWER) {
            resetElectionTimer();
        }
    }

    public Role getState() { return state; }
    public long getCurrentTerm() { return currentTerm; }
    public Address getCurrentLeader() { return currentLeader; }

    private synchronized void resetElectionTimer() {
        if (electionTimerFuture != null) {
            electionTimerFuture.cancel(true);
        }
        int timeout = 150 + random.nextInt(150); // 150ms to 300ms
        electionTimerFuture = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private synchronized void startElection() {
        if (state == Role.LEADER || shard == null) {
            return;
        }

        state = Role.CANDIDATE;
        currentTerm++;
        votedFor = selfAddress;
        votesReceived = 1;
        currentLeader = null;

        System.out.println("[" + selfAddress + "] Started election for term " + currentTerm);
        resetElectionTimer();

        List<Address> peers = getPeerAddresses();
        if (peers.isEmpty() || votesReceived > (getClusterSize() / 2)) {
            becomeLeader();
            return;
        }

        long lastLogIndex = log.size();
        long lastLogTerm = lastLogIndex > 0 ? log.get((int) lastLogIndex - 1).getTerm() : 0;
        Command req = Command.createRequestVote(currentTerm, selfAddress.toString(), lastLogIndex, lastLogTerm);

        for (Address peer : peers) {
            asyncRpcExecutor.submit(() -> sendRpc(peer, req));
        }
    }

    private synchronized void becomeLeader() {
        state = Role.LEADER;
        currentLeader = selfAddress;
        if (electionTimerFuture != null) electionTimerFuture.cancel(true);

        System.out.println("[" + selfAddress + "] Became LEADER for term " + currentTerm);
        heartbeatTimerFuture = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);
    }

    public synchronized Command handleRequestVote(Command req) {
        long term = req.getTerm();
        if (term > currentTerm) {
            currentTerm = term;
            state = Role.FOLLOWER;
            votedFor = null;
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
        }

        boolean voteGranted = false;
        if (term == currentTerm && (votedFor == null || votedFor.toString().equals(req.getCandidateId()))) {
            long lastLogIndex = log.size();
            long lastLogTerm = lastLogIndex > 0 ? log.get((int) lastLogIndex - 1).getTerm() : 0;

            long reqLastLogIndex = req.getLastLogIndex() != null ? req.getLastLogIndex() : 0;
            long reqLastLogTerm = req.getLastLogTerm() != null ? req.getLastLogTerm() : 0;

            if (reqLastLogTerm > lastLogTerm || (reqLastLogTerm == lastLogTerm && reqLastLogIndex >= lastLogIndex)) {
                voteGranted = true;
                votedFor = parseAddress(req.getCandidateId());
                resetElectionTimer();
            }
        }

        return Command.createRequestVoteResponse(currentTerm, voteGranted);
    }

    public synchronized void handleRequestVoteResponse(Command resp, Address sender) {
        if (resp.getTerm() > currentTerm) {
            currentTerm = resp.getTerm();
            state = Role.FOLLOWER;
            votedFor = null;
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
            resetElectionTimer();
            return;
        }

        if (state == Role.CANDIDATE && resp.getTerm() == currentTerm && Boolean.TRUE.equals(resp.getVoteGranted())) {
            votesReceived++;
            if (votesReceived > (getClusterSize() / 2)) {
                becomeLeader();
            }
        }
    }

    public synchronized Command handleAppendEntries(Command req) {
        long term = req.getTerm();
        if (term > currentTerm) {
            currentTerm = term;
            state = Role.FOLLOWER;
            votedFor = null;
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
        }

        if (term < currentTerm) {
            return Command.createAppendEntriesResponse(currentTerm, false, log.size());
        }

        state = Role.FOLLOWER;
        currentLeader = parseAddress(req.getLeaderId());
        resetElectionTimer();

        if (req.getEntries() != null && !req.getEntries().isEmpty()) {
            for (RaftLogEntry entry : req.getEntries()) {
                log.add(entry);
                applyEntry(entry);
            }
        }

        if (req.getLeaderCommit() != null && req.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(req.getLeaderCommit(), log.size());
        }

        return Command.createAppendEntriesResponse(currentTerm, true, log.size());
    }

    public synchronized void handleAppendEntriesResponse(Command resp, Address sender) {
        if (resp.getTerm() > currentTerm) {
            currentTerm = resp.getTerm();
            state = Role.FOLLOWER;
            votedFor = null;
            if (heartbeatTimerFuture != null) heartbeatTimerFuture.cancel(true);
            resetElectionTimer();
        }
    }

    private synchronized void sendHeartbeats() {
        if (state != Role.LEADER || shard == null) return;

        Command heartbeat = Command.createAppendEntries(currentTerm, selfAddress.toString(), log.size(),
                log.size() > 0 ? log.get(log.size() - 1).getTerm() : 0, null, commitIndex);

        for (Address peer : getPeerAddresses()) {
            asyncRpcExecutor.submit(() -> sendRpc(peer, heartbeat));
        }
    }

    public synchronized void appendAndApply(String type, String key, String value) {
        long index = log.size() + 1;
        RaftLogEntry entry = new RaftLogEntry(index, currentTerm, type, key, value);
        log.add(entry);
        applyEntry(entry);
        commitIndex = index;
    }

    private void applyEntry(RaftLogEntry entry) {
        if ("PUT".equals(entry.getType())) {
            store.put(entry.getKey(), entry.getValue());
        } else if ("DELETE".equals(entry.getType())) {
            store.remove(entry.getKey());
        }
        lastApplied = entry.getIndex();
    }

    private void sendRpc(Address peer, Command command) {
        ConnectionPool pool = connectionPoolManager.getPool(peer);
        NodeConnection conn = null;
        try {
            conn = pool.borrow();
            conn.out.println(mapper.writeValueAsString(command));
            String responseStr = conn.in.readLine();
            if (responseStr != null) {
                Command resp = mapper.readValue(responseStr, Command.class);
                if ("REQUEST_VOTE_RESP".equals(resp.getType())) {
                    handleRequestVoteResponse(resp, peer);
                } else if ("APPEND_ENTRIES_RESP".equals(resp.getType())) {
                    handleAppendEntriesResponse(resp, peer);
                }
            }
            pool.release(conn);
        } catch (IOException e) {
            if (conn != null) conn.close();
        }
    }

    private List<Address> getPeerAddresses() {
        if (shard == null) return Collections.emptyList();
        List<Address> peers = new ArrayList<>(shard.getFollowers());
        if (shard.getLeader() != null) peers.add(shard.getLeader());
        peers.remove(selfAddress);
        return peers;
    }

    private int getClusterSize() {
        if (shard == null) return 1;
        int count = shard.getFollowers().size();
        if (shard.getLeader() != null) count++;
        return Math.max(1, count);
    }

    private Address parseAddress(String addrStr) {
        if (addrStr == null || !addrStr.contains(":")) return null;
        String[] parts = addrStr.split(":");
        return new Address(parts[0], Integer.parseInt(parts[1]));
    }
}


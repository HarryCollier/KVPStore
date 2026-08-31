// Node.java
import java.io.*;
import java.net.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.*;

public class Node {
    //for converting command objects to json and vice versa
    private static final ObjectMapper mapper = new ObjectMapper();
    
    //prevents too many threads being spawned for incoming client connections
    private static final ExecutorService serverThreadPool = Executors.newFixedThreadPool(200);
    // prevents thread starvation by handling outgoing node-to-node replication separately
    private static final ExecutorService replicationThreadPool = Executors.newCachedThreadPool();
    
    // connection pool manager, limiting number of connections, and preventing creation overhead
    private static final NodeConnectionPoolManager connectionPoolManager = new NodeConnectionPoolManager(30);
    //the shard this node is in, to be filled in by reciving a hearbeat from the router
    //volatile: set by whichever connection thread handles the heartbeat, read by every
    //request-handling and replication thread
    private static volatile Shard shard;
    // the address this node is running on, to be filled in by the main method
    private static Address address;
    //append only log file for this node
    private static BufferedWriter logWriter;
    // path to the action log, kept so we can count existing lines on startup
    private static final String LOG_PATH = "data/node.log";
    // the kv store itself - now a field (was a main() local) so catch-up code
    // running outside respond() can reach it
    private static SimpleKVPStore store;

    // this node's current position in the action log. On the leader this is the
    // authoritative "next offset to assign" counter. On a follower it's "the last
    // offset I've actually applied" - only ever touched while holding storeLock.
    private static long logOffset = 0;

    // guards every read/mutation of the store + logOffset + log file together,
    // so a snapshot/catch-up apply can never interleave with a live replicated
    // write (or with the leader assigning offsets while a SYNC read is in flight).
    private static final Object storeLock = new Object();

    // true once this node has a store consistent with the leader's. Leader sets
    // this true immediately (it can't be behind itself). Followers start false
    // and flip it once a SYNC round-trip completes; gap detection can flip it
    // back to false if a live write arrives out of order.
    private static volatile boolean caughtUp = false;

    // true while a SYNC request is in flight and its response hasn't been
    // applied yet. Live replicated writes that arrive during this window get
    // buffered instead of applied, so they can't race the snapshot being applied.
    private static volatile boolean syncInProgress = false;

    // live replicated writes received while syncInProgress is true, or while
    // waiting out a gap-triggered resync. Drained (in offset order) right after
    // the sync response is applied.
    private static final List<Command> pendingBuffer = Collections.synchronizedList(new ArrayList<>());

    // ensures only one catch-up attempt is in flight at a time
    private static final AtomicBoolean catchUpTriggered = new AtomicBoolean(false);
    

    public static void main(String[] args) throws Exception {
        // if too few args entered, return error
        if (args.length < 3) {
            System.err.println("ERROR: missing arguments for PORTNUMBER PROPERTIESFILE SHARDNUMBER");
            System.exit(1);
        }
        //get the port and filename from args
        int port = Integer.parseInt(args[0]);
        String host = System.getenv().getOrDefault("NODE_HOST", "localhost");
        address = new Address(host, port);
        String fileName = args[1];
        
        //using try so it auto closes the serverSocket
        try (ServerSocket serverSocket = new ServerSocket(address.port)) {
            
            //initiate KVP store
            store = new SimpleKVPStore(fileName);

            // make sure the data directory exists before we try to open a file in it
            new File("data").mkdirs();

            // count existing entries so we know where to resume numbering from -
            // this is what makes the log "pick up where it left off" after a restart
            logOffset = countExistingLogLines(LOG_PATH);
            //System.out.println("Resuming action log at offset " + logOffset);

            //initiate log writer - append mode, so restarting never overwrites past entries
            logWriter = new BufferedWriter(new FileWriter(LOG_PATH, true));

            //System.out.println("Server Started...");

            while (true) {
                //wait for client
                Socket client = serverSocket.accept();
                //System.out.println("Client Connected");
                //submit a new thread to handle client (using the dedicated server pool)

                serverThreadPool.submit(() -> {
                    try {
                        //initiate read/writes
                        BufferedReader in = new BufferedReader (
                            new InputStreamReader(client.getInputStream())
                        );
                        PrintWriter out = new PrintWriter(
                            client.getOutputStream(), true
                        );


                        //take input
                        String jsonRequest;
                        while((jsonRequest = in.readLine()) != null) {

                            //server logs
                            //System.out.println("Server recieved request: " + jsonRequest);

                            //parse req into generic tree first, allowing us to inspect fields and determine type of req
                            JsonNode node = mapper.readTree(jsonRequest);
                            Command command;
                            //if contains a type then its a command
                            if (node.has("type")) {
                                if (shard == null) {
                                    out.println("Node not ready yet, try again shortly");
                                    //System.out.println("Node not ready yet, try again shortly");
                                } else {
                                    //parse into command object
                                    command = mapper.readValue(jsonRequest, Command.class);
                                    //respond to request
                                    respond(command, in, out);
                                }
                            } else {
                                //if not a command then its a replication request (heartbeat)
                                Shard oldShard = shard;
                                shard = mapper.readValue(jsonRequest, Shard.class);
                                //respond to replication request
                                //System.out.println("Updated shard");
                                out.println("Shard for address " + address + " updated");

                                if (address.equals(shard.getLeader())) {
                                    // a leader is trivially caught up with itself
                                    caughtUp = true;
                                } else if (!caughtUp) {
                                    // first heartbeat ever, or a previous catch-up attempt
                                    // hasn't succeeded yet - (re)try on every heartbeat until it does
                                    triggerCatchUp();
                                }
                            }
                        }
                        

                        client.close();
                    }
                    catch (IOException e) {
                        //System.out.println("Error with client: " + e.getMessage());
                        try{client.close();}
                        catch (IOException ex) {
                            //System.out.println("Error closing client: " + ex.getMessage());
                        }
                    }
                
                });
            } 
        }catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
        
    }

    /**
     * @param req the request as a string
     * * takes a request, processes it, and carries out necicary actions
     */
    private static void respond(Command command, BufferedReader in, PrintWriter out) {
        
        //process the inputted command

        //get type key and value from command
        String type = command.getType();
        String key = command.getKey();
        String value = command.getValue();

        //System.out.println("Address is " + address.prettyPrint() + " and shard is " + shard.prettyPrint());

        //if command is a put
        if (type.equals("PUT")) {
            if (address.equals(shard.getLeader())) {
                long assignedOffset;
                synchronized (storeLock) {
                    store.put(key, value);
                    assignedOffset = writeLog("PUT", key, value);
                }
                // stamp the offset we just assigned before fanning it out
                forwardReqToNodes(new Command("PUT", key, value, assignedOffset));
                out.println("Input stored successfully");
                //System.out.println("Request stored sucessfully");
            } else {
                // this PUT arrived as a replicated write forwarded by the leader
                handleReplicatedWrite(command);
                out.println("Input stored successfully");
            }

        } // if the command type is a get
        else if (type.equals("GET")) {
            //if port is the leader of the shard, forward the request to the other nodes in the shard
            if (address.equals(shard.getLeader())) {
                String response = sendToRandomNode(command);
                out.println(response);
            //get the value from the store
            } else {
                if (!caughtUp) {
                    // don't ever hand back a wrong/missing answer while mid catch-up
                    out.println("Not caught up yet, try again shortly");
                    //System.out.println("Rejected GET for " + key + " - not caught up");
                } else {
                    value = store.get(key);
                    //if val exists, output it, if not error msg
                    if (value != null) {
                        out.println("Found value: " + value);
                        //System.out.println("Response sent sucessfully where value:" + value);

                    }
                    else {
                        out.println("No value found...");
                        //System.out.println("No value found for key: " + key);
                    }
                }
            }
        } //if the command is a delete
        else if (type.equals("DELETE")) {
            if (address.equals(shard.getLeader())) {
                //delete that kvp from store
                boolean removed;
                long assignedOffset;
                synchronized (storeLock) {
                    removed = store.remove(key);
                    if (removed) {
                        assignedOffset = writeLog("DELETE", key, "");
                    } else {
                        assignedOffset = -1;
                    }
                }
                if (removed) {
                    forwardReqToNodes(new Command("DELETE", key, "", assignedOffset));
                    //trigers if a key was removed
                    out.println("Removed key: " + key +" from store");
                    //System.out.println("KVP sucessfully deleted");
                } else {
                    //runs if key not removed
                    out.println("Key was not found in the store");
                    //System.out.println("KVP not found");
                }
            } else {
                // replicated delete forwarded by the leader - it already confirmed
                // the key existed there, so just apply it (idempotent if we somehow
                // already don't have the key)
                handleReplicatedWrite(command);
                out.println("Removed key: " + key + " from store");
                //System.out.println("KVP sucessfully deleted");
            }
        }
        // new: a follower asking to catch up. Carries the follower's current
        // offset in command.getOffset(); we hand back every log entry after it.
        else if (type.equals("SYNC")) {
            if (!address.equals(shard.getLeader())) {
                out.println("Not the leader, cannot serve SYNC");
                //System.out.println("Rejected SYNC request - this node isn't the leader");
            } else {
                long followerOffset = command.getOffset() != null ? command.getOffset() : 0L;
                List<Command> entries = readLogEntriesAfter(followerOffset);
                try {
                    out.println(mapper.writeValueAsString(entries));
                    //System.out.println("Served SYNC from offset " + followerOffset + ": " + entries.size() + " entries");
                } catch (IOException e) {
                    System.err.println("Error serializing sync response: " + e.getMessage());
                    out.println("[]");
                }
            }
        }
        
        // if command type is not recognised
        else {
                out.println("Enter valid command: GET/DELETE KEY or PUT KEY VALUE");
        }
        
    }

    /**
     * Applies a single replicated PUT/DELETE arriving from the leader. Handles
     * three cases: a sync is currently in flight (buffer it - Hole 3), it's
     * exactly the next offset we expect (apply it), or there's a gap (we missed
     * something earlier - buffer this one and kick off a resync to fill it).
     */
    private static void handleReplicatedWrite(Command command) {
        synchronized (storeLock) {
            if (syncInProgress) {
                pendingBuffer.add(command);
                return;
            }

            Long incomingOffset = command.getOffset();
            if (incomingOffset == null) {
                System.err.println("Replicated write missing offset, ignoring: " + command.getType() + " " + command.getKey());
                return;
            }
            if (incomingOffset <= logOffset) {
                // stale duplicate (e.g. re-sent after a connection hiccup) - already applied
                //System.out.println("Ignoring stale replicated write at offset " + incomingOffset + " (already at " + logOffset + ")");
                return;
            }
            if (incomingOffset > logOffset + 1) {
                // we're missing something before this - don't apply out of order
                //System.out.println("Gap detected: have " + logOffset + ", got " + incomingOffset + " - triggering resync");
                pendingBuffer.add(command);
                triggerCatchUp();
                return;
            }

            applyEntry(command);
        }
    }

    /**
     * Applies one already-numbered log entry (from a SYNC response, the pending
     * buffer, or a directly-next replicated write) to the store and the log.
     * Caller must hold storeLock.
     */
    private static void applyEntry(Command entry) {
        String type = entry.getType();
        if ("PUT".equals(type)) {
            store.put(entry.getKey(), entry.getValue());
        } else if ("DELETE".equals(type)) {
            store.remove(entry.getKey());
        }
        appendLogLine(entry.getOffset(), type, entry.getKey(), entry.getValue());
        logOffset = entry.getOffset();
    }

    /**
     * Kicks off (or leaves alone, if one's already running) a catch-up attempt.
     * Safe to call while already holding storeLock, since the actual work runs
     * on a separate thread.
     */
    private static void triggerCatchUp() {
        if (!catchUpTriggered.compareAndSet(false, true)) {
            return; // an attempt is already in flight
        }
        caughtUp = false;
        syncInProgress = true;
        replicationThreadPool.submit(() -> {
            try {
                catchUpFromLeader();
            } finally {
                catchUpTriggered.set(false);
            }
        });
    }

    /**
     * Sends a SYNC request to the leader carrying our current offset, then
     * applies whatever comes back. On any failure, just leaves caughtUp/syncInProgress
     * false so the next heartbeat (or next gap) retries it.
     */
    private static void catchUpFromLeader() {
        if (shard == null || address.equals(shard.getLeader())) {
            // self-check: never try to catch up from ourselves
            syncInProgress = false;
            return;
        }

        Address leader = shard.getLeader();
        long myOffset;
        synchronized (storeLock) {
            myOffset = logOffset;
        }

        String json;
        try {
            json = mapper.writeValueAsString(new Command("SYNC", null, null, myOffset));
        } catch (IOException e) {
            System.err.println("Error serializing SYNC request: " + e.getMessage());
            syncInProgress = false;
            return;
        }

        ConnectionPool pool = connectionPoolManager.getPool(leader);
        NodeConnection conn = null;
        String response;
        try {
            conn = pool.borrow();
            conn.out.println(json);
            response = conn.in.readLine();
            pool.release(conn);
        } catch (IOException e) {
            System.err.println("Error requesting sync from leader: " + e.getMessage());
            if (conn != null) conn.close();
            syncInProgress = false;
            return;
        }

        if (response == null) {
            System.err.println("Leader closed connection during sync");
            syncInProgress = false;
            return;
        }

        applySyncResponse(response);
    }

    /**
     * Parses the leader's SYNC response, applies the entries in order, then
     * drains anything buffered while the sync was in flight (deduping against
     * what the snapshot already covered), and finally flips caughtUp.
     */
    private static void applySyncResponse(String response) {
        // A successful SYNC always returns a JSON array. Anything else is one of
        // the leader's plain-text guard responses (e.g. "Node not ready yet...",
        // "Not the leader, cannot serve SYNC") - not a real failure, just means
        // this attempt didn't land; leave caughtUp/syncInProgress false so the
        // next heartbeat retries it rather than trying to parse prose as JSON.
        if (response == null || !response.trim().startsWith("[")) {
            System.err.println("Sync attempt did not return data, will retry: " + response);
            syncInProgress = false;
            return;
        }

        List<Command> entries;
        try {
            entries = mapper.readValue(response, new TypeReference<List<Command>>() {});
        } catch (IOException e) {
            System.err.println("Error parsing sync response: " + e.getMessage());
            syncInProgress = false;
            return;
        }

        synchronized (storeLock) {
            entries.sort(Comparator.comparingLong(Command::getOffset));
            for (Command entry : entries) {
                if (entry.getOffset() != null && entry.getOffset() > logOffset) {
                    applyEntry(entry);
                }
            }

            List<Command> buffered;
            synchronized (pendingBuffer) {
                buffered = new ArrayList<>(pendingBuffer);
                pendingBuffer.clear();
            }
            buffered.sort(Comparator.comparingLong(Command::getOffset));
            for (Command buf : buffered) {
                if (buf.getOffset() != null && buf.getOffset() > logOffset) {
                    applyEntry(buf);
                }
            }

            caughtUp = true;
            syncInProgress = false;
        }
        System.out.println("Caught up to offset " + logOffset);
    }

    /**
     * Reads the on-disk log and returns every entry after the given offset, as
     * Command objects ready to serialize back to a syncing follower. Holds
     * storeLock so it can't read a torn line mid-write.
     */
    private static List<Command> readLogEntriesAfter(long offset) {
        List<Command> result = new ArrayList<>();
        synchronized (storeLock) {
            try (BufferedReader reader = new BufferedReader(new FileReader(LOG_PATH))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ", 4);
                    if (parts.length < 3) continue;
                    long entryOffset;
                    try {
                        entryOffset = Long.parseLong(parts[0]);
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    if (entryOffset <= offset) continue;
                    String entryType = parts[1];
                    String entryKey = parts[2];
                    String entryValue = parts.length > 3 ? parts[3] : "";
                    result.add(new Command(entryType, entryKey, entryValue, entryOffset));
                }
            } catch (IOException e) {
                System.err.println("Error reading log for sync: " + e.getMessage());
            }
        }
        return result;
    }

    /**
     * Counts how many entries already exist in the action log, so a restarted
     * node knows what offset to resume numbering from instead of starting at 0
     * and colliding with entries it already logged before it stopped.
     */
    private static long countExistingLogLines(String path) {
        File f = new File(path);
        if (!f.exists()) {
            return 0;
        }
        long count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            while (reader.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            System.err.println("Error counting existing log lines: " + e.getMessage());
        }
        return count;
    }

    /**
     * Increments this node's log counter and appends the entry under that new
     * number. Only called on the leader path (which is the only place a new
     * offset should ever be minted); caller must already hold storeLock.
     */
    private static long writeLog(String type, String key, String value) {
        logOffset++;
        appendLogLine(logOffset, type, key, value);
        return logOffset;
    }

    private static void appendLogLine(long offset, String type, String key, String value) {
        try {
            String line = offset + " " + type + " " + key + (value != null && !value.isEmpty() ? " " + value : "");
            logWriter.write(line);
            logWriter.newLine();
            logWriter.flush();
        } catch (IOException e) {
            System.err.println("Error writing to log: " + e.getMessage());
        }
    }


    private static void sendToNode(Address nodeAddress, String message) {
        ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);

        for (int attempt = 0; attempt < 2; attempt++) {
            NodeConnection conn = null;
            try {
                //System.out.println("Sending to node " + nodeAddress + ": " + message);
                conn = pool.borrow();
                conn.out.println(message);

                String response = conn.in.readLine();
                if (response == null) {
                    //System.out.println("Node " + nodeAddress + " connection closed (EOF), discarding");
                    conn.close();
                    continue;
                }

                pool.release(conn);
                return;

            } catch (IOException e) {
                //System.out.println("Node " + nodeAddress + " unreachable: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                if (conn != null) {
                    conn.close();
                }
            }
        }

        //System.out.println("Node " + nodeAddress + " unreachable after retry, giving up for this cycle");
    }

    private static void forwardReqToNodes(Command command) {
        //if shard is null dont forward req
        if (shard == null) {
            //System.out.println("Cannot take requests, shard not set yet");
            return;
        }
        // convert command to json - done once as to reduce overhead of converting multiple times in the loop
        String json;
        try {
            json = mapper.writeValueAsString(command);
        } catch (IOException e) {
            System.err.println("Error serializing command: " + e.getMessage());
            return;
        }

        // submit all sends in parallel, collecting a Future for each
        List<Future<?>> futures = new ArrayList<>();
        //System.out.println("Forwarding request to followers of shard " + shard.getId());
        for (Address nodeAddress : shard.getFollowers()) {
                //add future to list (using the dedicated replication pool)
                //System.out.println("Forwarding request to node " + nodeAddress.getPort() + ": " + json);
                Future<?> future = replicationThreadPool.submit(() -> {sendToNode(nodeAddress, json);});
                futures.add(future);
            
        }
        //wait for all futures to finish
        for (Future<?> future : futures) {
            try {
                future.get(); //blocks until this task is done
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                System.err.println("Task failed: " + e.getCause());
            }
        }
    }


    // Sends a GET to a random follower; if that follower isn't caught up (or is
    // unreachable), tries the others before giving up - so a not-ready follower
    // doesn't get to be the final answer a client sees.
    private static String sendToRandomNode(Command command) {
        if (shard.getFollowers().isEmpty()) {
            //System.out.println("No followers to send to.");
            return "No followers available";
        }

        List<Address> followers = new ArrayList<>(shard.getFollowers());
        Collections.shuffle(followers);

        String req;
        try {
            req = mapper.writeValueAsString(command);
        } catch (IOException e) {
            System.err.println("Error serializing command: " + e.getMessage());
            return "Error serializing command";
        }

        String lastResult = "No followers available";
        for (Address nodeAddress : followers) {
            ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);

            for (int attempt = 0; attempt < 2; attempt++) {
                NodeConnection conn = null;
                try {
                    conn = pool.borrow();
                    conn.out.println(req);
                    //System.out.println("Sent request to node " + nodeAddress);
                    String result = conn.in.readLine();

                    if (result == null) {
                        //System.out.println("Node " + nodeAddress + " connection closed (EOF), discarding");
                        conn.close();
                        continue;
                    }

                    pool.release(conn);

                    if (!result.equals("Not caught up yet, try again shortly")) {
                        return result;
                    }
                    lastResult = result;
                    break;

                } catch (IOException e) {
                    //System.out.println("Node " + nodeAddress + " unreachable: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    lastResult = "Node unreachable";
                    if (conn != null) {
                        conn.close();
                    }
                    break;
                }
            }
        }
        return lastResult;
    }
}
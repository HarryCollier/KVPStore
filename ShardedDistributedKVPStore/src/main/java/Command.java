// Command.java
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

public class Command {
    private String type;
    private String key;
    private String value;

    // Leader-assigned global sequence number. Null on plain client GET/PUT/DELETE
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long offset;

    // Raft Consensus fields
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long term;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String candidateId;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long lastLogIndex;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long lastLogTerm;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean voteGranted;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String leaderId;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long prevLogIndex;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long prevLogTerm;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<RaftLogEntry> entries;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long leaderCommit;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean success;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long matchIndex;

    /**
     * basic constructor for reflection when converting to json
     */
    public Command() {}

    /**
     * @param type the type of the command
     * @param key the key of the KVP
     * @param value the value of the KVP
     */
    public Command(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    /**
     * Same as above, plus an explicit offset
     */
    public Command(String type, String key, String value, Long offset) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.offset = offset;
    }

    /**
     * Factory for RequestVote RPC
     */
    public static Command createRequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        Command c = new Command();
        c.type = "REQUEST_VOTE";
        c.term = term;
        c.candidateId = candidateId;
        c.lastLogIndex = lastLogIndex;
        c.lastLogTerm = lastLogTerm;
        return c;
    }

    /**
     * Factory for RequestVoteResponse RPC
     */
    public static Command createRequestVoteResponse(long term, boolean voteGranted) {
        Command c = new Command();
        c.type = "REQUEST_VOTE_RESP";
        c.term = term;
        c.voteGranted = voteGranted;
        return c;
    }

    /**
     * Factory for AppendEntries RPC (Heartbeat & Log Replication)
     */
    public static Command createAppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                             List<RaftLogEntry> entries, long leaderCommit) {
        Command c = new Command();
        c.type = "APPEND_ENTRIES";
        c.term = term;
        c.leaderId = leaderId;
        c.prevLogIndex = prevLogIndex;
        c.prevLogTerm = prevLogTerm;
        c.entries = entries;
        c.leaderCommit = leaderCommit;
        return c;
    }

    /**
     * Factory for AppendEntriesResponse RPC
     */
    public static Command createAppendEntriesResponse(long term, boolean success, long matchIndex) {
        Command c = new Command();
        c.type = "APPEND_ENTRIES_RESP";
        c.term = term;
        c.success = success;
        c.matchIndex = matchIndex;
        return c;
    }

    /**
     * parses a command string from client input
     */
    public Command(String request) {
        if (request == null || request.trim().isEmpty()) {
            throw new IllegalArgumentException("Null/Empty request received");
        }

        String[] data = request.split(" ", 3);
        if (data.length < 2) {
            throw new IllegalArgumentException("Server only accepts inputs in the form: GET/DELETE KEY or PUT KEY VALUE");
        }
        this.type = data[0];
        this.key = data[1];
        this.value = data.length < 3 ? "" : data[2];
    }

    // GETTERS & SETTERS
    public String getType() { return this.type; }
    public void setType(String type) { this.type = type; }

    public String getKey() { return this.key; }
    public void setKey(String key) { this.key = key; }

    public String getValue() { return this.value; }
    public void setValue(String value) { this.value = value; }

    public Long getOffset() { return this.offset; }
    public void setOffset(Long offset) { this.offset = offset; }

    public Long getTerm() { return term; }
    public void setTerm(Long term) { this.term = term; }

    public String getCandidateId() { return candidateId; }
    public void setCandidateId(String candidateId) { this.candidateId = candidateId; }

    public Long getLastLogIndex() { return lastLogIndex; }
    public void setLastLogIndex(Long lastLogIndex) { this.lastLogIndex = lastLogIndex; }

    public Long getLastLogTerm() { return lastLogTerm; }
    public void setLastLogTerm(Long lastLogTerm) { this.lastLogTerm = lastLogTerm; }

    public Boolean getVoteGranted() { return voteGranted; }
    public void setVoteGranted(Boolean voteGranted) { this.voteGranted = voteGranted; }

    public String getLeaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }

    public Long getPrevLogIndex() { return prevLogIndex; }
    public void setPrevLogIndex(Long prevLogIndex) { this.prevLogIndex = prevLogIndex; }

    public Long getPrevLogTerm() { return prevLogTerm; }
    public void setPrevLogTerm(Long prevLogTerm) { this.prevLogTerm = prevLogTerm; }

    public List<RaftLogEntry> getEntries() { return entries; }
    public void setEntries(List<RaftLogEntry> entries) { this.entries = entries; }

    public Long getLeaderCommit() { return leaderCommit; }
    public void setLeaderCommit(Long leaderCommit) { this.leaderCommit = leaderCommit; }

    public Boolean getSuccess() { return success; }
    public void setSuccess(Boolean success) { this.success = success; }

    public Long getMatchIndex() { return matchIndex; }
    public void setMatchIndex(Long matchIndex) { this.matchIndex = matchIndex; }
}
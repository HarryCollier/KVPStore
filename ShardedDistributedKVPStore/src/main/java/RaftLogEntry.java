import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftLogEntry {
    private long index;
    private long term;
    private String type;
    private String key;
    private String value;

    public RaftLogEntry() {}

    @JsonCreator
    public RaftLogEntry(
            @JsonProperty("index") long index,
            @JsonProperty("term") long term,
            @JsonProperty("type") String type,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value) {
        this.index = index;
        this.term = term;
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public long getIndex() { return index; }
    public void setIndex(long index) { this.index = index; }

    public long getTerm() { return term; }
    public void setTerm(long term) { this.term = term; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
}


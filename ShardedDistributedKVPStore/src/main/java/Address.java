import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class Address {
    final String host;
    final int port;

    @JsonCreator
    public Address(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    public Address() {
        this.host = null;
        this.port = 0;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Address)) return false;
        Address other = (Address) o;
        return port == other.port && Objects.equals(host, other.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    public String prettyPrint() {
        return "Address{host='" + host + "', port=" + port + "}";
    }
}
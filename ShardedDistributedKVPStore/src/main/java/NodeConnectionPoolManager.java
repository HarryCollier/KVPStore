import java.util.concurrent.*;

public class NodeConnectionPoolManager {
    private final ConcurrentHashMap<Integer, ConnectionPool> poolMap = new ConcurrentHashMap<>();
    private final String host;
    private final int maxPerNode;

    public NodeConnectionPoolManager(String host, int maxPerNode) {
        this.host = host;
        this.maxPerNode = maxPerNode;
    }

    public ConnectionPool getPool(int port) {
        return poolMap.computeIfAbsent(port, p -> new ConnectionPool(host, p, maxPerNode));
    }

    public void shutdownAll(){
        poolMap.values().forEach(ConnectionPool::shutdown);
    }
}
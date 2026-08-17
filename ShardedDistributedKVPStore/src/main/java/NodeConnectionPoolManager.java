import java.util.concurrent.*;

public class NodeConnectionPoolManager {
    private final ConcurrentHashMap<Address, ConnectionPool> poolMap = new ConcurrentHashMap<>();
    private final int maxPerNode;

    public NodeConnectionPoolManager(int maxPerNode) {
        this.maxPerNode = maxPerNode;
    }

    public ConnectionPool getPool(Address address) {
        return poolMap.computeIfAbsent(address, a -> new ConnectionPool(a, maxPerNode));
    }

    public void shutdownAll() {
        poolMap.values().forEach(ConnectionPool::shutdown);
    }
}
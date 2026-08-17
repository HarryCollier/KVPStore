import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ConnectionPool {
    private final Address address;
    private final int maxSize;
    private final BlockingQueue<NodeConnection> pool;
    private boolean isShutdown = false;

    public ConnectionPool(Address address, int maxSize) {
        this.address = address;
        this.maxSize = maxSize;
        this.pool = new LinkedBlockingQueue<>(maxSize);
    }

    public NodeConnection borrow() throws IOException {
        NodeConnection conn = pool.poll();

        while (conn != null && !conn.isAlive()) {
            conn.close();
            conn = pool.poll();
        }

        if (conn == null) {
            conn = new NodeConnection(address.host, address.port);
        }

        return conn;
    }

    public void release(NodeConnection conn) {
        if (conn.isAlive() && pool.size() < maxSize && !isShutdown) {
            pool.offer(conn);
        } else {
            conn.close();
        }
    }

    public void shutdown() {
        NodeConnection conn;
        isShutdown = true;
        while ((conn = pool.poll()) != null) {
            conn.close();
        }
    }
}
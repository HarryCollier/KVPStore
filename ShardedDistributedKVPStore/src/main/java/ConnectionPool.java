import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ConnectionPool{
    private final String host;
    private final int port;
    private final int maxSize;
    private final BlockingQueue<NodeConnection> pool;
    private boolean isShutdown = false;

    public ConnectionPool(String host, int port, int maxSize) {
        this.host = host;
        this.port = port;
        this.maxSize = maxSize;
        this.pool = new LinkedBlockingQueue<>(maxSize);
    }

    public NodeConnection borrow() throws IOException {
        NodeConnection conn = pool.poll();

        while(conn != null && !conn.isAlive()) {
            conn.close();
            conn = pool.poll();
        }

        if (conn == null) {
            conn = new NodeConnection(host, port);
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
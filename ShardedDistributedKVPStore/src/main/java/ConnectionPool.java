import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ConnectionPool {
    private final Address address;
    private final int maxSize;
    private final BlockingQueue<NodeConnection> pool;
    // actually bounds concurrent connections to this node - borrow() blocks
    // (briefly) if we're already at maxSize outstanding, instead of just
    // minting a new raw socket every time the idle cache happens to be empty
    private final Semaphore permits;
    private boolean isShutdown = false;

    public ConnectionPool(Address address, int maxSize) {
        this.address = address;
        this.maxSize = maxSize;
        this.pool = new LinkedBlockingQueue<>(maxSize);
        this.permits = new Semaphore(maxSize, false);
    }

    public NodeConnection borrow() throws IOException {
        try {
            // wait for a permit rather than creating an unbounded number of
            // fresh sockets when concurrency exceeds maxSize
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for a connection permit", e);
        }

        NodeConnection conn = pool.poll();

        while (conn != null && !conn.isAlive()) {
            conn.close();
            conn = pool.poll();
        }

        if (conn == null) {
            try {
                conn = new NodeConnection(address.host, address.port);
            } catch (IOException e) {
                permits.release(); // give the permit back - we never actually used it
                throw e;
            }
        }

        return conn;
    }

    public void release(NodeConnection conn) {
        if (conn.isAlive() && pool.size() < maxSize && !isShutdown) {
            pool.offer(conn);
        } else {
            conn.close();
        }
        permits.release();
    }

    public void shutdown() {
        NodeConnection conn;
        isShutdown = true;
        while ((conn = pool.poll()) != null) {
            conn.close();
        }
    }
}
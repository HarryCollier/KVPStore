import java.io.*;
import java.net.*;


public class NodeConnection {
    final Socket socket;
    final PrintWriter out;
    final BufferedReader in;

    public NodeConnection(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    public boolean isAlive() {
        return socket.isConnected() && !socket.isClosed();
    }

    public void close() {
        try {
            in.close();
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
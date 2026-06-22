import java.io.*;
import java.net.*;

public class SimpleClient {
    /**
     * simple client to communicate with a port
     */
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 8080);

        BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream())
        );

        PrintWriter out = new PrintWriter(
            socket.getOutputStream(), true
        );

        out.println("hello server");

        String response = in.readLine();
        System.out.println("Server said: " + response);

        socket.close();
    }
}
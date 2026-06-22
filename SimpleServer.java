import java.io.*;
import java.net.*;

public class SimpleServer {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);

        System.out.println("Server started...");

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client connected!");

            BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream())
            );

            PrintWriter out = new PrintWriter(
                client.getOutputStream(), true
            );

            String message = in.readLine();
            System.out.println("Received: " + message);

            out.println("Echo: " + message);

            client.close();
        }
    }
}
import java.io.*;
import java.net.*;

public class MultiServer {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);

        while (true) {
            Socket client = serverSocket.accept();

            new Thread(() -> {
                try {
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream())
                    );

                    PrintWriter out = new PrintWriter(
                        client.getOutputStream(), true
                    );

                    String msg = in.readLine();
                    out.println("Handled: " + msg);
                    System.out.println("Req recieved:" + msg);

                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
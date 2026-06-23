import java.io.*;
import java.net.*;

public class Router {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);

        System.out.println("Server started...");

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client accepted");

            new Thread(() ->{
                try {
                    Socket node = new Socket("localhost", 8081);

                    PrintWriter outNode = new PrintWriter(
                        node.getOutputStream(), true
                    );

                    BufferedReader inNode = new BufferedReader (
                        new InputStreamReader(node.getInputStream())
                    );


                    PrintWriter outClient = new PrintWriter(
                        client.getOutputStream(), true
                    );

                    BufferedReader inClient = new BufferedReader(
                        new InputStreamReader(client.getInputStream())
                    );

                    
                    System.out.println("I/O initialised, about to begin reading");
                    String request = inClient.readLine();
                    System.out.println("read line");
                    
                    outNode.println(request);
                    System.out.println("req sent to node");

                    String response = inNode.readLine();
                    System.out.println("reponse read from node");

                    outClient.println(response);
                    System.out.println("response sent to client");

                    node.close();
                    client.close();

                }
                catch (IOException e){
                    System.out.println("Error in router transmissions: " + e.getMessage());
                }
            }).start();
        }
    }
}
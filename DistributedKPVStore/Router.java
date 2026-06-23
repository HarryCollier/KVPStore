import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Router {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);

        List<String> nodes = Files.readAllLines(Paths.get("cluster.conf"));

        int[] nodesPorts = new int[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            String pNum = nodes.get(i).split(" ")[0];
            nodesPorts[i] = Integer.parseInt(pNum); 
        }


        System.out.println("Server started...");

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client accepted");

            new Thread(() ->{
                try {
                    Socket node = new Socket("localhost", nodesPorts[0]);

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

                    
                    String request = inClient.readLine();
                    
                    outNode.println(request);

                    String response = inNode.readLine();

                    outClient.println(response);

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
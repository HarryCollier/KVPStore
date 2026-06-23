import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Router {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        //using try so it auto closes the server socket
        try (ServerSocket serverSocket = new ServerSocket(8080)) {

            //get all nodes in the cluster.conf file
            List<String> nodes = Files.readAllLines(Paths.get("cluster.conf"));
            
            //initiate the ring
            ConsisentHashRing ring = new ConsisentHashRing(100);
            //go through nodes and add each ports number to the ring
            for (int i = 0; i < nodes.size(); i++) {
                String pNum = nodes.get(i).split(" ")[0];
                ring.add(Integer.parseInt(pNum));
            }


            System.out.println("Server started...");
            //loop forever
            while (true) {
                //wait for a client
                Socket client = serverSocket.accept();
                System.out.println("Client accepted");
        
                //set I/Os
                PrintWriter outClient = new PrintWriter(
                    client.getOutputStream(), true
                );

                BufferedReader inClient = new BufferedReader(
                    new InputStreamReader(client.getInputStream())
                );


                new Thread(() ->{
                    try {
                        //get request
                        String request = inClient.readLine();
                        //convert request from a string to a Command
                        Command command = null;
                        try {
                            //make command out of request
                            command = new Command(request);
                        } catch (IllegalArgumentException e) {
                            //wrong arguments, so error, and close client
                            System.out.println("Error parsing request");
                            outClient.println(e.getMessage());
                            client.close();
                            return;
                        }

                        //get the port number that is repsonsible for this command
                        int nodesPnum = ring.getNode(command.getKey());
                        //make a socket communicating with this port
                        Socket node = new Socket("localhost", nodesPnum);

                        //set I/Os
                        PrintWriter outNode = new PrintWriter(
                            node.getOutputStream(), true
                        );

                        BufferedReader inNode = new BufferedReader (
                            new InputStreamReader(node.getInputStream())
                        );

                        //convert command object into a JSON
                        String jsonRequest = mapper.writeValueAsString(command);

                        //send json to Node
                        outNode.println(jsonRequest);

                        //get response
                        String response = inNode.readLine();

                        //send response to client
                        outClient.println(response);
                        
                        //close node and client
                        node.close();
                        client.close();

                    }
                    catch (IOException e){
                        System.out.println("Error in router transmissions: " + e.getMessage());
                    }
                }).start();
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }
}
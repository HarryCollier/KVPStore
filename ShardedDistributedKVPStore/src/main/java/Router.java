import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.*;


public class Router {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ExecutorService pool = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {
        

        //using try so it auto closes the server socket
        try (ServerSocket serverSocket = new ServerSocket(8080)) {

            //get all nodes in the cluster.conf file
            List<String> nodes = Files.readAllLines(Paths.get("cluster.conf"));
            
            //initiate the ring
            ConsisentHashRing ring = new ConsisentHashRing(100);
            //initiate a map of ids to shards
            Map<Integer, Shard> shardMap = new HashMap<>();
            //go through nodes and add each ports number to the ring
            for (int i = 0; i < nodes.size(); i++) {
                //split line into data
                String[] line = nodes.get(i).split(" ");
                int shardId = Integer.parseInt(line[2]);
                int nodesPNum = Integer.parseInt(line[0]);
                //if the shard is in the map, add this node as a follower, if it isnt then add a new shard to the map and the ring, with its leader being this node
                shardMap.compute(shardId, (key, currV) -> {
                    if (currV == null) {
                        Shard shard = new Shard(shardId,nodesPNum);
                        ring.add(shardId);
                        return shard;
                    } else {
                        currV.addFollower(nodesPNum);
                        return currV;
                    }
                });

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


                pool.submit(() ->{
                    try {
                        Command command = null;

                        //get request
                        String request;
                        while((request = inClient.readLine()) != null) {

                            
                            //convert request from a string to a Command
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
                            //get the shardId that is repsonsible for this command
                            int shardId = ring.getNode(command.getKey());
                            //get the shard
                            Shard shard = shardMap.get(shardId);
                            //get the leader of the shard
                            int leader = shard.getLeader();

                            //set up socket and I/Os to the leader node
                            try (Socket node = new Socket("localhost", leader);
                            PrintWriter outNode = new PrintWriter(node.getOutputStream(), true);
                            BufferedReader inNode = new BufferedReader(new InputStreamReader(node.getInputStream()));) 
                                {

                                //convert command object into a JSON
                                String jsonRequest = mapper.writeValueAsString(command);

                                //send json to Node
                                outNode.println(jsonRequest);
                                //get response
                                String response = inNode.readLine();
                                //send response to client
                                outClient.println(response);
                                //close node and client
                                }
                        }
                    }
                    catch (IOException e){
                        System.out.println("Error in router transmissions: " + e.getMessage());
                    }
                });
            }
        }
        catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }
}

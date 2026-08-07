import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.*;


public class Router {

    private static final ObjectMapper mapper = new ObjectMapper();
    //thread pool limiting number of threads
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(10);
    //connection pool, limiting number of connections, and preventing creation overhead
    private static final NodeConnectionPoolManager connectionPoolManager = new NodeConnectionPoolManager("localhost", 30);

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

            //spawn new thread, should be seperate from pool, as only spawned once, and will run forever
            new Thread(() -> {

                // compute targets once, before entering the loop
                Map<Shard, List<Integer>> shardTargets = new HashMap<>();
                for (Shard shard : shardMap.values()) {
                    List<Integer> targets = new ArrayList<>(shard.getFollowers());
                    targets.add(shard.getLeader());
                    shardTargets.put(shard, targets);
                }

                // loop is now dedicated purely to sending
                while (true) {
                    System.out.println("Sending shard info to nodes...");
                    for (Map.Entry<Shard, List<Integer>> entry : shardTargets.entrySet()) {
                        for (int nodePort : entry.getValue()) {
                            threadPool.submit(() -> {
                                try {
                                    sendToNode(nodePort, mapper.writeValueAsString(entry.getKey()));
                                } catch (IOException e) {
                                    System.err.println("Error sending shard info to node " + nodePort + ": " + e.getMessage());
                                }
                            });
                        }
                    }

                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }).start();

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


                threadPool.submit(() ->{
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

                            //get the correct pool for this leader (creating if it doesnt exist)
                            ConnectionPool pool = connectionPoolManager.getPool(leader);

                            //borrow a connection from the pool
                            NodeConnection nodeConn = pool.borrow();

                            //convert command object into a JSON
                            String jsonRequest = mapper.writeValueAsString(command);

                            //send json to Node
                            nodeConn.out.println(jsonRequest);
                            //get response
                            String response = nodeConn.in.readLine();
                            //send response to client
                            outClient.println(response);
                            //close node and client
                        
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





    private static void sendToNode(int nodePort, String message) {
        ConnectionPool pool = connectionPoolManager.getPool(nodePort);
        NodeConnection conn = null;
        try {
            conn = pool.borrow();
            conn.out.println(message);

            // handle response
            String response = conn.in.readLine();
            pool.release(conn);

        } catch (IOException e) {
            System.out.println("Node " + nodePort + " unreachable: " + e.getMessage());
            
        }
    }
}

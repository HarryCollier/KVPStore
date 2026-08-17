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
    // dedicated pool for long-lived client connections
    private static final ExecutorService clientThreadPool = Executors.newFixedThreadPool(20);
    // dedicated pool for short-lived heartbeat sends, so a stuck client or node connection
    // can never block heartbeats (or vice versa)
    private static final ExecutorService heartbeatThreadPool = Executors.newCachedThreadPool();
    //connection pool, limiting number of connections, and preventing creation overhead
    private static final NodeConnectionPoolManager connectionPoolManager = new NodeConnectionPoolManager(30);
    public static void main(String[] args) throws Exception {
        

        //using try so it auto closes the server socket
        try (ServerSocket serverSocket = new ServerSocket(8080)) {

            //get all nodes in the cluster.conf file
            // Get node topology from environment variable instead of cluster.conf
            String rawNodes = System.getenv().getOrDefault("CLUSTER_NODES", "");
            if (rawNodes.trim().isEmpty()) {
                System.err.println("ERROR: CLUSTER_NODES environment variable is not set.");
                System.exit(1);
            }

            String[] nodes = rawNodes.split(",");

            ConsisentHashRing ring = new ConsisentHashRing(100);
            Map<Integer, Shard> shardMap = new HashMap<>();

            for (String nodeEntry : nodes) {
                if (nodeEntry.trim().isEmpty()) continue;
                
                // Parse "node1:8081:1" into host, port, and shardId
                String[] line = nodeEntry.trim().split(":");
                String host = line[0];
                int nodesPNum = Integer.parseInt(line[1]);
                int shardId = Integer.parseInt(line[2]);
                Address nodeAddress = new Address(host, nodesPNum);

                shardMap.compute(shardId, (key, currV) -> {
                    if (currV == null) {
                        Shard shard = new Shard(shardId, nodeAddress);
                        ring.add(nodeAddress);
                        return shard;
                    } else {
                        currV.addFollower(nodeAddress);
                        return currV;
                    }
                });
            }

            //spawn new thread, should be seperate from pool, as only spawned once, and will run forever
            new Thread(() -> {

                // compute targets once, before entering the loop
                Map<Shard, List<Address>> shardTargets = new HashMap<>();
                for (Shard shard : shardMap.values()) {
                    List<Address> targets = new ArrayList<>(shard.getFollowers());
                    targets.add(shard.getLeader());
                    shardTargets.put(shard, targets);
                }

                // loop is now dedicated purely to sending
                while (true) {
                    System.out.println("Sending shard info to nodes...");
                    for (Map.Entry<Shard, List<Address>> entry : shardTargets.entrySet()) {
                        for (Address nodeAddress : entry.getValue()) {
                            heartbeatThreadPool.submit(() -> {
                                try {
                                    sendToNode(nodeAddress, mapper.writeValueAsString(entry.getKey()));
                                } catch (IOException e) {
                                    System.err.println("Error sending shard info to node " + nodeAddress + ": " + e.getMessage());
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


                clientThreadPool.submit(() ->{
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
                            //get the node Address that is responsible for this command
                            Address nodeAddress = ring.getNode(command.getKey());

                            if (nodeAddress == null) {
                                outClient.println("No node available");
                                continue;
                            }

                            //get the correct pool for this leader (creating if it doesnt exist)
                            ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);

                            //borrow a connection from the pool
                            NodeConnection nodeConn = pool.borrow();

                            //convert command object into a JSON
                            String jsonRequest = mapper.writeValueAsString(command);

                            try {
                                //send json to Node
                                nodeConn.out.println(jsonRequest);
                                //get response
                                String response = nodeConn.in.readLine();
                                //send response to client
                                outClient.println(response);
                                //release connection back to pool
                                pool.release(nodeConn);
                            } catch (IOException e) {
                                // node didn't respond in time - don't kill the whole client
                                // connection over one bad node, and don't reuse the broken connection
                                System.out.println("Node " + nodeAddress + " unreachable: " + e.getClass().getSimpleName());
                                nodeConn.close();
                                outClient.println("Node unreachable, try again");
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




    
    private static void sendToNode(Address nodeAddress, String message) {
        ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);
        NodeConnection conn = null;
        try {
            conn = pool.borrow();
            conn.out.println(message);

            // handle response
            String response = conn.in.readLine();
            pool.release(conn);

        } catch (IOException e) {
            System.out.println("Node " + nodeAddress + " unreachable: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            // don't leak or reuse a connection that just failed - close it instead of releasing it
            if (conn != null) {
                conn.close();
            }
        }
    }
}
import java.io.*;
import java.net.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.*;
import java.util.concurrent.*;
import java.util.*;

public class Node {
    //for converting command objects to json and vice versa
    private static final ObjectMapper mapper = new ObjectMapper();
    
    //prevents too many threads being spawned for incoming client connections
    private static final ExecutorService serverThreadPool = Executors.newFixedThreadPool(10);
    // prevents thread starvation by handling outgoing node-to-node replication separately
    private static final ExecutorService replicationThreadPool = Executors.newCachedThreadPool();
    
    // connection pool manager, limiting number of connections, and preventing creation overhead
    private static final NodeConnectionPoolManager connectionPoolManager = new NodeConnectionPoolManager(30);
    //the shard this node is in, to be filled in by reciving a hearbeat from the router
    private static Shard shard;
    // the address this node is running on, to be filled in by the main method
    private static Address address;
    

    public static void main(String[] args) throws Exception {
        // if too few args entered, return error
        if (args.length < 3) {
            System.err.println("ERROR: missing arguments for PORTNUMBER PROPERTIESFILE SHARDNUMBER");
            System.exit(1);
        }
        //get the port and filename from args
        int port = Integer.parseInt(args[0]);
        address = new Address("localhost", port);
        String fileName = args[1];
        
        //using try so it auto closes the serverSocket
        try (ServerSocket serverSocket = new ServerSocket(address.port)) {
            
            //initiate KVP store
            SimpleKVPStore store = new SimpleKVPStore(fileName);

            System.out.println("Server Started...");

            while (true) {
                //wait for client
                Socket client = serverSocket.accept();
                System.out.println("Client Connected");
                //submit a new thread to handle client (using the dedicated server pool)

                serverThreadPool.submit(() -> {
                    try {
                        //initiate read/writes
                        BufferedReader in = new BufferedReader (
                            new InputStreamReader(client.getInputStream())
                        );
                        PrintWriter out = new PrintWriter(
                            client.getOutputStream(), true
                        );


                        //take input
                        String jsonRequest;
                        while((jsonRequest = in.readLine()) != null) {

                            //server logs
                            System.out.println("Server recieved request: " + jsonRequest);

                            //parse req into generic tree first, allowing us to inspect fields and determine type of req
                            JsonNode node = mapper.readTree(jsonRequest);
                            Command command;
                            //if contains a type then its a command
                            if (node.has("type")) {
                                if (shard == null) {
                                    out.println("Node not ready yet, try again shortly");
                                } else {
                                    //parse into command object
                                    command = mapper.readValue(jsonRequest, Command.class);
                                    //respond to request
                                    respond(command, in, out, store);
                                }
                            } else {
                                //if not a command then its a replication request
                                shard = mapper.readValue(jsonRequest, Shard.class);
                                //respond to replication request
                                System.out.println("Updated shard");
                                out.println("Shard for address " + address + " updated");
                            }
                        }
                        

                        client.close();
                    }
                    catch (IOException e) {
                        System.out.println("Error with client: " + e.getMessage());
                        try{client.close();}
                        catch (IOException ex) {
                            System.out.println("Error closing client: " + ex.getMessage());
                        }
                    }
                
                });
            } 
        }catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
        
    }

    /**
     * @param req the request as a string
     * * takes a request, processes it, and carries out necicary actions
     */
    private static void respond(Command command, BufferedReader in, PrintWriter out, SimpleKVPStore store) {
        
        //process the inputted command

        //get type key and value from command
        String type = command.getType();
        String key = command.getKey();
        String value = command.getValue();

        //if command is a put
        if (type.equals("PUT")) {
            store.put(key, value);
            if (address.equals(shard.getLeader())) {
                // Fixed: replicate write to ALL followers, not just a random one
                forwardReqToNodes(command);
            }

            out.println("Input stored successfully");
            System.out.println("Request stored sucessfully");

        } // if the command type is a get
        else if (type.equals("GET")) {
            //if port is the leader of the shard, forward the request to the other nodes in the shard
            if (address.equals(shard.getLeader())) {
                System.out.println("Leader node received GET request, forwarding to a follower...");
                // Fixed: capture the response from the follower and send it back to the client
                String response = sendToRandomNode(command);
                out.println(response);
            //get the value from the store
            } else {
                value = store.get(key);
                //if val exists, output it, if not error msg
                if (value != null) {
                    out.println("Found value: " + value);
                    System.out.println("Response sent sucessfully where value:" + value);

                }
                else {
                    out.println("No value found...");
                    System.out.println("No value found for key: " + key);
                }
            }
        } //if the command is a delete
        else if (type.equals("DELETE")) {
            //delete that kvp from store
            if (store.remove(key)) {
                if (address.equals(shard.getLeader())) {
                    forwardReqToNodes(command);
                }
                //trigers if a key was removed
                out.println("Removed key: " + key +" from store");
                System.out.println("KVP sucessfully deleted");
            }
            else {
                //runs if key not removed
                out.println("Key was not found in the store");
                System.out.println("KVP not found");
            }
            }
        
        // if command type is not recognised
        else {
                out.println("Enter valid command: GET/DELETE KEY or PUT KEY VALUE");
        }
        
    }


    private static void sendToNode(Address nodeAddress, String message) {
        
        //get connection from pool
        NodeConnection conn = null;
        ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);
        try {
            conn = pool.borrow();
            //send message to node
            conn.out.println(message);
            // handle response
            conn.in.readLine(); // Read the response from the node (if needed)
            // only a connection that made it through a full request/response cycle is safe to reuse
            pool.release(conn);
        } catch (IOException e) {
            System.err.println("Error sending message to node: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            // connection failed mid-flight - don't put it back in the pool, close it instead
            if (conn != null) {
                conn.close();
            }
        }

    }

    private static void forwardReqToNodes(Command command) {
        //if shard is null dont forward req
        if (shard == null) {
            System.out.println("Cannot take requests, shard not set yet");
            return;
        }
        // convert command to json - done once as to reduce overhead of converting multiple times in the loop
        String json;
        try {
            json = mapper.writeValueAsString(command);
        } catch (IOException e) {
            System.err.println("Error serializing command: " + e.getMessage());
            return;
        }

        // submit all sends in parallel, collecting a Future for each
        List<Future<?>> futures = new ArrayList<>();
        for (Address nodeAddress : shard.getFollowers()) {
                //add future to list (using the dedicated replication pool)
                Future<?> future = replicationThreadPool.submit(() -> {sendToNode(nodeAddress, json);});
                futures.add(future);
            
        }
        //wait for all futures to finish
        for (Future<?> future : futures) {
            try {
                future.get(); //blocks until this task is done
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                System.err.println("Task failed: " + e.getCause());
            }
        }
    }


    // Fixed: Changed return type to String
    private static String sendToRandomNode(Command command) {
        Address nodeAddress;
        if (shard.getFollowers().isEmpty()) {
            System.out.println("No followers to send to.");
            return "No followers available";
        } else {
            
            //select a random follower
            Random rand = new Random();
            int randomIndex = rand.nextInt(shard.getFollowers().size());
            nodeAddress = shard.getFollowers().get(randomIndex);
        }


        //convert command to a string for sending to node
        String req;
        try {
            req = mapper.writeValueAsString(command);
        } catch (IOException e) {
            System.err.println("Error serializing command: " + e.getMessage());
            return "Error serializing command";
        }


        //get connection from pool
        ConnectionPool pool = connectionPoolManager.getPool(nodeAddress);
        NodeConnection conn = null;
        String result; // Added variable to hold the response
        try {
            conn = pool.borrow();

            conn.out.println(req);
            System.out.println("Sent request to node " + nodeAddress);
            result = conn.in.readLine(); // Read the response from the node (if needed)
            System.out.println("Response from node " + nodeAddress + ": " + result);
            pool.release(conn);
        } catch (IOException e) {
            System.out.println("Node " + nodeAddress + " unreachable: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            result = "Node unreachable";
            if (conn != null) {
                conn.close();
            }
        }
        
        return result; // Return the read response back to the caller
    }
}
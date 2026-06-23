import java.io.*;
import java.net.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Node {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // if too few args entered, return error
        if (args.length < 2) {
            System.err.println("ERROR: missing arguments for PORTNUMBER and PROPERTIESFILE");
            System.exit(1);
        }
        //get the port and filename from args
        int port = Integer.parseInt(args[0]);
        String fileName = args[1];
        //using try so it auto closes the serverSocket
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            
            //initiate KVP store
            SimpleKVPStore store = new SimpleKVPStore(fileName);

            System.out.println("Server Started...");

            while (true) {
                //wait for client
                Socket client = serverSocket.accept();
                System.out.println("Client Connected");
                //start a new thread to deal with this client
                new Thread(() -> {
                    try {
                        //initiate read/writes
                        BufferedReader in = new BufferedReader (
                            new InputStreamReader(client.getInputStream())
                        );
                        PrintWriter out = new PrintWriter(
                            client.getOutputStream(), true
                        );


                        //take input
                        String jsonRequest = in.readLine();

                        //server logs
                        System.out.println("Server recieved request: " + jsonRequest);

                        //turn jsonRequest into a real command object
                        Command command = mapper.readValue(jsonRequest, Command.class);

                        //respond to request
                        respond(command, in, out, store);

                        client.close();
                    }
                    catch (IOException e) {
                        System.out.println("Error with client: " + e.getMessage());
                    }
                
                }).start();
            } 
        }catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
        
    }

    /**
     * @param req the request as a string
     * 
     * takes a request, processes it, and carries out necicary actions
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
                out.println("Input stored successfully");
                System.out.println("Request stored sucessfully");

            } // if the command type is a get
            else if (type.equals("GET")) {
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
            } //if the command is a delete
            else if (type.equals("DELETE")) {
                //delete that kvp from store
                if (store.remove(key)) {
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
}

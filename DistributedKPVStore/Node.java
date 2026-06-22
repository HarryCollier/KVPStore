import java.io.*;
import java.net.*;

import java.util.Arrays;

public class Node {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("ERROR: missing arguments for PORTNUMBER and PROPERTIESFILE");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String fileName = args[1];

        ServerSocket serverSocket = new ServerSocket(port);
        
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
                    String req = in.readLine();

                    //server logs
                    System.out.println("Server recieved request: " + req);

                    //respond to request
                    respond(req, in, out, store);

                    client.close();
                }
                catch (IOException e) {
                    System.out.println("Error with client: " + e.getMessage());
                }
            
            }).start();

        }
    }

    /**
     * @param req the request as a string
     * 
     * takes a request, processes it, and carries out necicary actions
     */
    private static void respond(String req, BufferedReader in, PrintWriter out, SimpleKVPStore store) {
            //decide if PUT or GET
            //split to extract command
            String[] splitReq = req.split(" ");
            

            //if length is less than 2 respond with error msg
            if (splitReq.length < 2) {
                out.println("Server only accepts inputs in the form: GET/DELETE KEY or PUT KEY VALUE");
            }

            // if command is a PUT
            else if (splitReq[0].equals("PUT")) {
                //get key and value
                String key = splitReq[1];
                //default val if no value was input
                String val = "";
                // extract val if it was input
                if (splitReq.length >= 3) {
                    //get everything from the request, except from 1st and 2nd words
                    val = String.join(" ", Arrays.copyOfRange(splitReq, 2, splitReq.length));
                }

                //store KVP
                store.put(key, val);

                //sucess messages
                out.println("Input stored successfully");
                System.out.println("Request stored sucessfully");
           }
           //if command is a GET
           else if (splitReq[0].equals("GET")) {
                //get key from req, and value from store
                String key = splitReq[1];
                String val = store.get(key);
                
                //if val exists, output it, if not error msg
                if (val != null) {
                    out.println("Found value: " + val);
                    System.out.println("Response sent sucessfully where value:" + val);

                }
                else {
                    out.println("No value found...");
                    System.out.println("No value found for key: " + key);
                }
           }
           //if the command is a delete
           else if (splitReq[0].equals("DELETE")) {
                //get key from req
                String key = splitReq[1];
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
           //if request not a command, but is of valid length
           else {
                out.println("Enter valid command: GET/DELETE KEY or PUT KEY VALUE");
           }
    }
}
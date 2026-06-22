import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

public class SimpleKVPServer {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);
        
        //initiate KVP store
        Map<String, String> store = new HashMap<>();

        System.out.println("Server Started...");

        while (true) {
            Socket client = serverSocket.accept();

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

            //decide if PUT or GET
            //split to extract command
            String[] splitReq = req.split(" ");
            

            //if length is less than 2 respond with error msg
            if (splitReq.length < 2) {
                out.println("Server only accepts inputs in the form: GET KEY or PUT KEY VALUE");
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
           else {
                out.println("Enter valid command: GET KEY or PUT KEY VALUE");
           }
           client.close();
        }
    }
}
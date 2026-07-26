import java.io.*;
import java.net.*;

public class Client {
    public static void main(String[] args) throws Exception{
        
        

        try (Socket socket = new Socket("localhost", 8080);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
            String command;
            for (int j = 0; j < 2000; j++) {
                int i = j % 100;
                if (i >= 95) {command = "PUT key" + i + " value" + i;}
                else {command = "GET key" + i;}
                //send command to router
                out.println(command);
                //read response
                String response = in.readLine();
                System.out.println("Response: " + response);
            }

        }


    }
}
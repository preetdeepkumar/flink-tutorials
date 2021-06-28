package org.pd.streaming.fraud;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BankDataServer {
    public static void main(String[] args) throws IOException{
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader br = null;
        Socket socket =null;
        try{
            System.out.println("I am going to simulate the transactions of a bank, reading entries and passing them through a socket...");
            socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            // I am going to simulate the transactions of a bank, reading entries and passing them through a socket...
            // in a real world, this process should a permanent broker pushing data to a topic...
            String path = "C:/data_fraud_flink_process/bank_data.txt";
            br = new BufferedReader(new FileReader(path));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;
            while((line = br.readLine()) != null){
                    out.println(line);
                    Thread.sleep(500);
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            cleaning_the_house(listener, br, socket);
        }
    }

    private static void cleaning_the_house(ServerSocket listener, BufferedReader br, Socket socket) throws IOException {
        if (socket !=null) socket.close();
        if (listener !=null) listener.close();
        if (br != null) br.close();
    }
}



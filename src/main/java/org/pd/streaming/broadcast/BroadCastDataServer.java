package org.pd.streaming.broadcast;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class BroadCastDataServer
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader br = null;
        Socket socket = null;
        try
        {
            System.out.println("BroadCastDataServer is online..." + listener.getInetAddress() + " " + listener.getLocalPort());
            socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            String fileName = "broadcast_small.txt";
            ClassLoader classLoader = BroadCastDataServer.class.getClassLoader();

            File file = new File(classLoader.getResource(fileName).getFile());
            br = new BufferedReader(new FileReader(file));

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;

                while((line = br.readLine()) != null)
                {
                    System.out.println(line);
                    out.println(line);
                    Thread.sleep(100);
                }
            

        } catch(Exception e )
        {
            e.printStackTrace();
        } finally
        {
            cleaning_the_house(listener, br, socket);
        }
        System.out.println("Done producing data...");
    }

    private static void cleaning_the_house(ServerSocket listener, BufferedReader br, Socket socket) throws IOException {
        if (listener !=null)
            listener.close();
        if (socket !=null)
            socket.close();
        if (br != null)
            br.close();
        System.out.println("House is clean...");
    }
}


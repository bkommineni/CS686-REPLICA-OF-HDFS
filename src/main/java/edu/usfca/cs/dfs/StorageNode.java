package edu.usfca.cs.dfs;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class StorageNode {

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        new StorageNode().start();
    }

    private void start() throws Exception
    {
        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("Listening...");
        while (true) {
            Socket socket = serverSocket.accept();
            StorageMessages.StoreChunk storeChunk
                    = StorageMessages.StoreChunk.parseDelimitedFrom(
                    socket.getInputStream());

            System.out.println("Storing file name: "
                    + storeChunk.getFileName());
        }
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

}

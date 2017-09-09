package edu.usfca.cs.dfs;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Controller {

    private List<Integer> storageNodesList = new ArrayList<>();

    public static void main(String[] args) throws Exception{
        String hostname = getHostname();
        System.out.println("Starting controller on " + hostname + "...");
        new Controller().start();
    }

    private void start() throws Exception
    {
        ServerSocket serverSocket = new ServerSocket(9998);
        storageNodesList.add(9999);
        System.out.println("Listening...");
        while (true)
        {
            Socket socket = serverSocket.accept();
            GetStorageNode.getStorageNode storageNode =
                    GetStorageNode.getStorageNode.newBuilder().setPort(9999).build();
            storageNode.writeDelimitedTo(socket.getOutputStream());

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

package edu.usfca.cs.dfs;

import com.google.protobuf.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Controller {

    /*
    Request Types
    1 Enroll request
    */

    private int controllerPort = 9998;
    private List<DataNode> storageNodesList = new ArrayList<>();
    private List<FileMetadata> fileMetadatas = new ArrayList<>();

    public static void main(String[] args) throws Exception{
        new Controller().start();
    }

    private void start() throws Exception
    {
        String hostname = getHostname();
        System.out.println("Starting controller on " + hostname + " and port: "+ controllerPort + "...");
        ServerSocket serverSocket = new ServerSocket(9998);
        System.out.println("Listening...");
        while (true)
        {
            Socket socket = serverSocket.accept();
            new Thread(new Request(socket)).start();
        }
    }

    public class Request implements Runnable {
        Socket connectionSocket = null;

        public Request(Socket connectionSocket) {
            this.connectionSocket = connectionSocket;
        }

        @Override
        public void run() {
            try {
                RequestsToController.RequestsToControllerWrapper msgWrapper = RequestsToController.RequestsToControllerWrapper
                                                                                .parseDelimitedFrom(connectionSocket.getInputStream());

                if(msgWrapper.hasEnrollMsg())
                {
                    //enroll storage node
                    storageNodesList.add(new DataNode(msgWrapper.getEnrollMsg().getPort()));
                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
                    acknowledgeEnrollment.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(msgWrapper.hasRetrieveFileRequestMsg())
                {
                    System.out.println("here in retrieve file!!");
                    //retrieve file functionality
                    ResponsesToClient.RetrieveFileResponseFromCN.storageNode storageNode = ResponsesToClient.RetrieveFileResponseFromCN.storageNode.newBuilder()
                                                                                            .setPort(9999).build();
                    ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN.newBuilder()
                                                                                    .setFilename(msgWrapper.getRetrieveFileRequestMsg().getFilename())
                                                                                    .addStorageNodeList(storageNode)
                                                                                    .build();
                    responseFromCN.writeDelimitedTo(connectionSocket.getOutputStream());
                    System.out.println("out of controller!!!");
                }

                if(msgWrapper.hasStoreChunkRequestMsg())
                {
                    //store file functionality
                    //allocate storage nodes for store file request
                    //when deploying on bass
                    Random rand = new Random();
                    int n = rand.nextInt(24) + 1;
                    System.out.println(n);
                    //return the set of nodes/numbers generated in response
                    ResponsesToClient.StoreChunkResponse.storageNode storageNode =
                            ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(9999).build();
                    ResponsesToClient.StoreChunkResponse storeChunkResponse =
                            ResponsesToClient.StoreChunkResponse.newBuilder()
                            .addStorageNodeList(storageNode).build();
                    storeChunkResponse.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(msgWrapper.hasAcknowledgeStoreChunkMsg())
                {
                    //acknowledge store chunk
                    //updating blocks and nodes info based on acknowledgement
                }

                if(msgWrapper.hasHeartbeatMsg())
                {
                    //check info sent on heartbeat and make sure what are active nodes
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
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

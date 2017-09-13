package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageNode {

    private int controllerPort = 9998;
    private int storageNodePort = 9999;

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        new StorageNode().start();
    }

    private void start() throws Exception
    {
        System.out.println("Enrolling with Controller after entering to network...");
        Socket connsocket = new Socket("localhost",controllerPort);
        RequestsToController.Enroll enroll = RequestsToController.Enroll.newBuilder()
                                                        .setPort(storageNodePort)
                                                        .build();
        RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                                                                    .newBuilder()
                                                                    .setEnrollMsg(enroll).build();

        wrapper.writeDelimitedTo(connsocket.getOutputStream());
        System.out.println("Waiting for Controller to acknowledge enrollment to start server...");
        ResponsesToStorageNode.AcknowledgeEnrollment response = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                        .parseDelimitedFrom(connsocket.getInputStream());
        System.out.println("Successfully enrolled with Controller!!");
        connsocket.close();
        System.out.println("Starting Storage Node on : "+ storageNodePort);
        if(response.getSuccess())
        {
            ServerSocket serverSocket = new ServerSocket(storageNodePort);
            System.out.println("Listening...");
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new Request(socket)).start();
            }
        }
    }

    public class Request implements Runnable {
        Socket connectionSocket = null;

        public Request(Socket connectionSocket) {
            this.connectionSocket = connectionSocket;
        }

        @Override
        public void run() {
            try
            {
                String currPath = ".";
                Path p = Paths.get(currPath);
                Path absDir = p.toAbsolutePath();
                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                                                                    .parseDelimitedFrom(connectionSocket.getInputStream());

                if(requestsWrapper.hasStoreChunkRequestToSNMsg())
                {
                    //Process the Request
                    System.out.println("store chunk SN!!!");
                    RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = requestsWrapper.getStoreChunkRequestToSNMsg();
                    System.out.println("Storing file name: "
                            + storeChunkRequestToSN.getFilename());

                    System.out.println(storeChunkRequestToSN.getChunkData());
                    System.out.println(storeChunkRequestToSN.getStorageNodeListList());

                    byte[] bytes = storeChunkRequestToSN.getChunkData().toByteArray();

                    int i=0;
                    String blockFile = absDir.toString() + "/data/" + storeChunkRequestToSN.getFilename() + "Part" + storeChunkRequestToSN.getChunkId() +".txt";
                    FileWriter writer = new FileWriter(blockFile);
                    while(i < bytes.length)
                    {
                        writer.write(bytes[i]);
                        i++;
                    }
                    writer.close();

                    ResponsesToClient.AcknowledgeStoreChunkToClient acknowledgeStoreChunkToClient = ResponsesToClient.AcknowledgeStoreChunkToClient.newBuilder()
                                                                                                    .setSuccess(true).build();
                    ResponsesToClient.ResponsesToClientWrapper wrapper = ResponsesToClient.ResponsesToClientWrapper.newBuilder()
                                                                        .setAcknowledgeStoreChunkToClientMsg(acknowledgeStoreChunkToClient).build();

                    wrapper.writeDelimitedTo(connectionSocket.getOutputStream());

                    //Send Response
                    /*ResponsesToController.AcknowledgeStoreChunkRequest acknowledgeStoreChunkRequest = ResponsesToController.AcknowledgeStoreChunkRequest
                                                                                                    .newBuilder()
                                                                                                    .setChunkId(storeChunkRequestToSN.getChunkId())
                                                                                                    .setFilename(storeChunkRequestToSN.getFilename())
                                                                                                    .setPort(storageNodePort)
                                                                                                    .build();
                    Socket socket = new Socket("localhost",controllerPort);
                    acknowledgeStoreChunkRequest.writeDelimitedTo(socket.getOutputStream());*/
                }

                if(requestsWrapper.hasRetrieveFileRequestToSNMsg())
                {
                    System.out.println("Retrieve file request in SN!!");
                    RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = requestsWrapper.getRetrieveFileRequestToSNMsg();
                    String filepath = absDir.toString() + "/data/";
                    byte[] chunkData = Files.readAllBytes(new File(filepath+ requestToSN.getFilename()+"Part"+requestToSN.getChunkId()+".txt").toPath());
                    System.out.println(requestToSN.getFilename());
                    System.out.println(chunkData.length);

                    ResponsesToClient.RetrieveFileResponseFromSN response = ResponsesToClient.RetrieveFileResponseFromSN.newBuilder()
                                                                            .setChecksum(0)
                                                                            .setFilename(requestToSN.getFilename())
                                                                            .setChunkId(requestToSN.getChunkId())
                                                                            .setChunkData(ByteString.copyFrom(chunkData)).build();
                    response.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(requestsWrapper.hasReadinessCheckRequestToSNMsg())
                {
                    ResponsesToClient.AcknowledgeReadinessToClient readinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.newBuilder()
                                                                                        .setSuccess(true).build();
                    readinessToClient.writeDelimitedTo(connectionSocket.getOutputStream());
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

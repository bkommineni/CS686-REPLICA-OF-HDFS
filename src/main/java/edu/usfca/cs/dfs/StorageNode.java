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
import java.util.ArrayList;
import java.util.List;

public class StorageNode {

    private int controllerPort = 9998;
    private String controllerPortHostName = "localhost";
    private int storageNodePort = 9999;

    public static void main(String[] args) 
    throws Exception {
        new StorageNode().start(args);
    }

    private void start(String[] args) throws Exception
    {
        if(args[1] != null) {
            storageNodePort = Integer.parseInt(args[1]);
            if(args[2] != null)
                controllerPort = Integer.parseInt(args[2]);
        }
        System.out.println("Enrolling with Controller after entering to network...");
        Socket connSocket = new Socket("localhost",controllerPort);
        RequestsToController.Enroll enroll = RequestsToController.Enroll.newBuilder()
                                                        .setPort(storageNodePort)
                                                        .setHostname(getHostname())
                                                        .build();
        RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                                                                    .newBuilder()
                                                                    .setEnrollMsg(enroll).build();

        wrapper.writeDelimitedTo(connSocket.getOutputStream());
        System.out.println("Waiting for Controller to acknowledge enrollment to start server...");
        ResponsesToStorageNode.AcknowledgeEnrollment response = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                        .parseDelimitedFrom(connSocket.getInputStream());
        System.out.println("Successfully enrolled with Controller!!");
        connSocket.close();
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

                    //Send Response to controller after store to update metadata info
                    RequestsToController.AcknowledgeStoreChunk acknowledgeStoreChunk = RequestsToController.AcknowledgeStoreChunk
                                                                                                    .newBuilder()
                                                                                                    .setChunkId(storeChunkRequestToSN.getChunkId())
                                                                                                    .setFilename(storeChunkRequestToSN.getFilename())
                                                                                                    .setPort(storageNodePort)
                                                                                                    .setHostname(InetAddress.getLocalHost().getHostName())
                                                                                                    .build();
                    RequestsToController.RequestsToControllerWrapper wrapper1 = RequestsToController.RequestsToControllerWrapper.newBuilder()
                                                                                    .setAcknowledgeStoreChunkMsg(acknowledgeStoreChunk).build();
                    Socket socket = new Socket(controllerPortHostName,controllerPort);
                    wrapper1.writeDelimitedTo(socket.getOutputStream());
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
                    new ReadinessCheckRequestToPeer(requestsWrapper.getReadinessCheckRequestToSNMsg()).run();

                    ResponsesToClient.AcknowledgeReadinessToClient readinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.newBuilder()
                                                                                        .setSuccess(true).build();
                    readinessToClient.writeDelimitedTo(connectionSocket.getOutputStream());
                }


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public class ReadinessCheckRequestToPeer implements Runnable
    {
        RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSNMsg = null;
        public ReadinessCheckRequestToPeer(RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSNMsg) {
            this.readinessCheckRequestToSNMsg = readinessCheckRequestToSNMsg;
        }

        @Override
        public void run() {
            try
            {
                String currPath = ".";
                Path p = Paths.get(currPath);
                Path absDir = p.toAbsolutePath();

                List<RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode> peerList = readinessCheckRequestToSNMsg.getStorageNodeListList();
                String filename = readinessCheckRequestToSNMsg.getFilename();
                int chunkId = readinessCheckRequestToSNMsg.getChunkId();
                String[] tokens = filename.split("/");
                int noOfTokens = tokens.length;
                String filePath = absDir.toString() + "/data/"+tokens[noOfTokens-1].split(".")[0]+"Part"+chunkId+".txt";

                Socket socket = new Socket(peerList.get(0).getHostname(),peerList.get(0).getPort());

                List<RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode> peers = new ArrayList<>();

                for(int i=1;i<peerList.size();i++)
                {
                    peers.add(peerList.get(i));
                }
                RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder();
                builder.addAllStorageNodeList(peers);
                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setReadinessCheckRequestToSNMsg(builder).build();
                requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                System.out.println("Waiting for response from peer Storage Node");
                ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                                                                                            .parseDelimitedFrom(socket.getInputStream());
                if(acknowledgeReadinessToSN.getSuccess())
                {
                    RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                                                                        .setFilename(filename)
                                                                                        .setChunkId(chunkId)
                                                                                        .setChunkData(ByteString.copyFrom(Files.readAllBytes(new File(filePath).toPath())))
                                                                                        .build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                                                                .newBuilder()
                                                                                .setStoreChunkRequestToSNMsg(storeChunkRequestToSN).build();
                    wrapper.writeDelimitedTo(socket.getOutputStream());
                }

            }
            catch (IOException e)
            {
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

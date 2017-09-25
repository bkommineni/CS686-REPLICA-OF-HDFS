package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class StorageNode {

    private int controllerPort = 9998;
    private String controllerPortHostName = "localhost";
    private int storageNodePort = 9999;
    private Map<String,StorageNodeMetadata> storageNodeMetadataMap = new HashMap<>();
    private Map<String,StorageNodeMetadata> dataStoredInLastFiveSeconds = new HashMap<>();
    private Socket connSocket = null;
    private Socket socket = null;

    public static void main(String[] args) 
    throws Exception
    {
        new StorageNode().start(args);
    }

    private void start(String[] args) throws Exception
    {
        if(args.length > 0) {
            if (args[0] != null) {
                controllerPortHostName = args[0] + ".cs.usfca.edu";
                if (args[1] != null)
                    controllerPort = Integer.parseInt(args[1]);
			if(args[2] != null)
				storageNodePort = Integer.parseInt(args[2]);
            }
        }
        System.out.println("Enrolling with Controller after entering to network...");
        connSocket = new Socket(controllerPortHostName,controllerPort);
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

        System.out.println("Starting Storage Node on : "+ storageNodePort);
        if(response.getSuccess())
        {
            ServerSocket serverSocket = new ServerSocket(storageNodePort);
            System.out.println("Listening...");
            while (true) {
                /*TimerTask task = new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            List<RequestsToController.Heartbeat.ChunkMetadata> chunkMetadataList = new ArrayList<>();
                            for (String key : dataStoredInLastFiveSeconds.keySet()) {
                                StorageNodeMetadata metadata = dataStoredInLastFiveSeconds.get(key);
                                RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = RequestsToController.Heartbeat.ChunkMetadata.newBuilder()
                                        .setChunkId(metadata.getChunkId())
                                        .setFilename(metadata.getFilename()).build();
                                chunkMetadataList.add(chunkMetadata);
                            }
                            RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                                    .setPort(storageNodePort)
                                    .setHostname(getHostname()).build();

                            RequestsToController.Heartbeat heartbeat = RequestsToController.Heartbeat.newBuilder()
                                    .addAllMetadata(chunkMetadataList)
                                    .setSN(storageNode)
                                    .build();
                            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                                    .setHeartbeatMsg(heartbeat).build();

                            wrapper.writeDelimitedTo(connSocket.getOutputStream());
                        }
                        catch (UnknownHostException e)
                        {
                            e.printStackTrace();
                        }
                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }
                    }
                };
                Timer timer = new Timer();
                long delay = 0;
                long intervalPeriod = 5 * 1000;
                timer.scheduleAtFixedRate(task,delay,intervalPeriod);*/
                socket = serverSocket.accept();
                new Thread(new Request(socket)).start();
            }
        }
    }

    public class Request implements Runnable
    {
        Socket connectionSocket = null;

        public Request(Socket connectionSocket)
        {
            this.connectionSocket = connectionSocket;
        }

        @Override
        public void run()
        {
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
                    System.out.println("Received Store chunk request from Client..");
                    RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = requestsWrapper.getStoreChunkRequestToSNMsg();
                    String filename = null;
                    int chunkId = 0;
                    String[] tokens = null;
                    int noOfTokens ;

                    byte[] bytes = null;

                    if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromClientMsg())
                    {
                        RequestsToStorageNode.StoreChunkRequestToSNFromClient storeChunkRequestToSNFromClient = storeChunkRequestToSN.getStoreChunkRequestToSNFromClientMsg();
                        filename = storeChunkRequestToSNFromClient.getFilename();
                        chunkId  = storeChunkRequestToSNFromClient.getChunkId();
                        tokens   =  filename.split("/");
                        noOfTokens = tokens.length;
                        tokens = tokens[noOfTokens - 1].split("\\.");
                        bytes =  storeChunkRequestToSNFromClient.getChunkData().toByteArray();
                    }
                    else if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromSNMsg())
                    {
                        RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSNFromSN = storeChunkRequestToSN.getStoreChunkRequestToSNFromSNMsg();
                        filename = storeChunkRequestToSNFromSN.getFilename();
                        chunkId  = storeChunkRequestToSNFromSN.getChunkId();
                        tokens   =  filename.split("/");
                        noOfTokens = tokens.length;
                        tokens = tokens[noOfTokens - 1].split("\\.");
                        bytes =  storeChunkRequestToSNFromSN.getChunkData().toByteArray();
                    }

                    /*Storing Chunk data on local file system of Node*/
                    int i=0;
		            String hostname = getHostname();
                    String[] tokens1 = hostname.split("\\.");
                    String blockFile = absDir.toString() + "/data/" + tokens[0] + "Part" + chunkId +"_"+ tokens1[0] +".txt";
                    FileWriter writer = new FileWriter(blockFile);
                    while(i < bytes.length)
                    {
                        writer.write(bytes[i]);
                        i++;
                    }
                    writer.close();

                    /*Calculating Checksum and adding all the chunkInfo(filename,chunkId,Checksum) to metadata Map of Storage Node*/
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    FileInputStream fis = new FileInputStream(blockFile);

                    byte[] dataBytes = new byte[1024];

                    int nread = 0;
                    while ((nread = fis.read(dataBytes)) != -1) {
                        md.update(dataBytes, 0, nread);
                    };
                    byte[] mdbytes = md.digest();
                    StorageNodeMetadata metadata = new StorageNodeMetadata(filename,chunkId);
                    metadata.setChecksum(mdbytes);
                    String key = filename + Integer.toString(chunkId);
                    storageNodeMetadataMap.put(key,metadata);
                    dataStoredInLastFiveSeconds.put(key,metadata);

                    if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromClientMsg())
                    {
                        ResponsesToClient.AcknowledgeStoreChunkToClient acknowledgeStoreChunkToClient = ResponsesToClient.AcknowledgeStoreChunkToClient.newBuilder()
                                .setSuccess(true).build();
                        ResponsesToClient.ResponsesToClientWrapper wrapper = ResponsesToClient.ResponsesToClientWrapper.newBuilder()
                                .setAcknowledgeStoreChunkToClientMsg(acknowledgeStoreChunkToClient).build();

                        wrapper.writeDelimitedTo(connectionSocket.getOutputStream());
                        System.out.println("Store Chunk done and sent a response to client");
                    }

                }

                if(requestsWrapper.hasRetrieveFileRequestToSNMsg())
                {
                    System.out.println("Received Retrieve file request from Client");
                    RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = requestsWrapper.getRetrieveFileRequestToSNMsg();
                    String filepath = absDir.toString() + "/data/";
                    String[] tokens = requestToSN.getFilename().split("/");
                    int length = tokens.length;
                    byte[] chunkData = Files.readAllBytes(new File(filepath+ tokens[length-1].split(".")[0]+"Part"+requestToSN.getChunkId()+".txt").toPath());

                    ResponsesToClient.RetrieveFileResponseFromSN response = ResponsesToClient.RetrieveFileResponseFromSN.newBuilder()
                                                                            .setChecksum(0)
                                                                            .setFilename(requestToSN.getFilename())
                                                                            .setChunkId(requestToSN.getChunkId())
                                                                            .setChunkData(ByteString.copyFrom(chunkData)).build();
                    response.writeDelimitedTo(connectionSocket.getOutputStream());
                    System.out.println("Retrieve chunk from this node is done and sent back reponse");
                }

                if(requestsWrapper.hasReadinessCheckRequestToSNMsg())
                {
                    RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN = requestsWrapper.getReadinessCheckRequestToSNMsg();
                    if(readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromClientMsg())
                    {
                        System.out.println("Received readiness check request from client");
                        ResponsesToClient.AcknowledgeReadinessToClient readinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.newBuilder()
                                .setSuccess(true).build();
                        readinessToClient.writeDelimitedTo(connectionSocket.getOutputStream());
                        System.out.println("Sent acknowledgement for readiness check to client");
                        System.out.println("Another Thread which sends request to other storage nodes in list ");
                        new ReadinessCheckRequestToPeer(readinessCheckRequestToSN).run();
                    }

                    if(readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromSNMsg())
                    {
                        System.out.println("Received readiness check request from Storage Node");
                        ResponsesToStorageNode.AcknowledgeReadinessToSN readinessToClient = ResponsesToStorageNode.AcknowledgeReadinessToSN.newBuilder()
                                .setSuccess(true).build();
                        readinessToClient.writeDelimitedTo(connectionSocket.getOutputStream());
                        System.out.println("Sent acknowledgement for readiness check to Storage Node");
                        System.out.println("Another Thread which sends request to other storage nodes in list ");
                        new ReadinessCheckRequestToPeer(readinessCheckRequestToSN).run();
                    }

                }

            }
            catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }
            catch (IOException e)
            {
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

                if(readinessCheckRequestToSNMsg.hasReadinessCheckRequestToSNFromClientMsg())
                {
                    RequestsToStorageNode.ReadinessCheckRequestToSNFromClient requestToSNFromClient = readinessCheckRequestToSNMsg.getReadinessCheckRequestToSNFromClientMsg();

                    while (requestToSNFromClient.getStorageNodeListList().size() > 0)
                    {
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode> peerList = requestToSNFromClient.getStorageNodeListList();
                        String filename = requestToSNFromClient.getFilename();
                        int chunkId = requestToSNFromClient.getChunkId();
                        String[] tokens = filename.split("/");
                        int noOfTokens = tokens.length;
                        tokens = tokens[noOfTokens - 1].split("\\.");

                        String filePath = absDir.toString() + "/data/" + tokens[0] + "Part" + chunkId + ".txt";

                        Socket socket = new Socket(peerList.get(0).getHostname(), peerList.get(0).getPort());
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peers = new ArrayList<>();
                        if(peerList.size() > 1)
                        {
                            for (int i = 1; i < peerList.size(); i++) {
                                RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode storageNode = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode.newBuilder()
                                                                                                                .setPort(peerList.get(i).getPort())
                                                                                                                .setHostname(peerList.get(i).getHostname()).build();
                                peers.add(storageNode);
                            }
                        }
                        RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.Builder builder1 = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.newBuilder()
                                                                                                    .addAllStorageNodeList(peers);
                        RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                                                                            .setReadinessCheckRequestToSNFromSNMsg(builder1);

                        RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                .setReadinessCheckRequestToSNMsg(builder).build();
                        requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                        System.out.println("Waiting for response from peer Storage Node");
                        ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                                .parseDelimitedFrom(socket.getInputStream());
                        if (acknowledgeReadinessToSN.getSuccess()) {
                            File file = new File(filePath);
                        /*while(!file.exists())
                        {
                            Thread.sleep(1000);
                        }*/
                            System.out.println("Store chunk request to peer storage node...  "+socket.getLocalPort()+" "+socket.getPort());
                            RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromSN.newBuilder()
                                    .setFilename(filename)
                                    .setChunkId(chunkId)
                                    .setChunkData(ByteString.copyFrom(Files.readAllBytes(file.toPath())))
                                    .build();
                            RequestsToStorageNode.StoreChunkRequestToSN requestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                                                                        .setStoreChunkRequestToSNFromSNMsg(storeChunkRequestToSN).build();
                            RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                    .newBuilder()
                                    .setStoreChunkRequestToSNMsg(requestToSN).build();
                            wrapper.writeDelimitedTo(socket.getOutputStream());
                            System.out.println("Received store chunk response from peer storage node...");
                        }
                    }
                }

                if(readinessCheckRequestToSNMsg.hasReadinessCheckRequestToSNFromSNMsg())
                {
                    RequestsToStorageNode.ReadinessCheckRequestToSNFromSN requestToSNFromSN = readinessCheckRequestToSNMsg.getReadinessCheckRequestToSNFromSNMsg();

                    while (requestToSNFromSN.getStorageNodeListList().size() > 0)
                    {
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peerList = requestToSNFromSN.getStorageNodeListList();
                        String filename = requestToSNFromSN.getFilename();
                        int chunkId = requestToSNFromSN.getChunkId();
                        String[] tokens = filename.split("/");
                        int noOfTokens = tokens.length;
                        tokens = tokens[noOfTokens - 1].split("\\.");

                        String filePath = absDir.toString() + "/data/" + tokens[0] + "Part" + chunkId + ".txt";

                        Socket socket = new Socket(peerList.get(0).getHostname(), peerList.get(0).getPort());
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peers = new ArrayList<>();
                        if(peerList.size() > 1)
                        {
                            for (int i = 1; i < peerList.size(); i++) {
                                RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode storageNode = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode.newBuilder()
                                        .setPort(peerList.get(i).getPort())
                                        .setHostname(peerList.get(i).getHostname()).build();
                                peers.add(storageNode);
                            }
                        }
                        RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.Builder builder1 = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.newBuilder()
                                .addAllStorageNodeList(peers);
                        RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                .setReadinessCheckRequestToSNFromSNMsg(builder1);

                        RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                .setReadinessCheckRequestToSNMsg(builder).build();
                        requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                        System.out.println("Waiting for response from peer Storage Node");
                        ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                                .parseDelimitedFrom(socket.getInputStream());
                        if (acknowledgeReadinessToSN.getSuccess()) {
                            File file = new File(filePath);
                        /*while(!file.exists())
                        {
                            Thread.sleep(1000);
                        }*/
                            System.out.println("Store chunk request to peer storage node...  "+socket.getLocalPort()+" "+socket.getPort());
                            RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromSN.newBuilder()
                                    .setFilename(filename)
                                    .setChunkId(chunkId)
                                    .setChunkData(ByteString.copyFrom(Files.readAllBytes(file.toPath())))
                                    .build();
                            RequestsToStorageNode.StoreChunkRequestToSN requestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                    .setStoreChunkRequestToSNFromSNMsg(storeChunkRequestToSN).build();
                            RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                    .newBuilder()
                                    .setStoreChunkRequestToSNMsg(requestToSN).build();
                            wrapper.writeDelimitedTo(socket.getOutputStream());
                            System.out.println("Received store chunk response from peer storage node...");
                        }
                    }
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

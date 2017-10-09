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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class StorageNode {

    public static final Logger logger = LoggerFactory.getLogger(StorageNode.class);
    private int controllerPort;
    private String controllerPortHostName = "localhost";
    private int storageNodePort;
    private Map<String,StorageNodeMetadata> storageNodeMetadataMap = new HashMap<>();
    private Map<String,StorageNodeMetadata> dataStoredInLastFiveSeconds = new HashMap<>();
    private Socket controllerSocket = null;
    private Socket connSocket = null;
    private String dataDirectory = "/home2/bkommineni/";

    public static void main(String[] args)
            throws Exception
    {
        new StorageNode().start(args);
    }

    private void start(String[] args) throws Exception
    {
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        //dataDirectory = absDir.toString() + "/data/";
        if(args.length > 0) {
            if (args[0] != null) {
                controllerPortHostName = args[0];
                if (args[1] != null)
                    controllerPort = Integer.parseInt(args[1]);
                if(args[2] != null)
                    storageNodePort = Integer.parseInt(args[2]);
                if(args[3] != null)
                    dataDirectory = absDir.toString() + args[3];
            }
        }
        logger.info("Enrolling with Controller {} on port {} after entering to network ",controllerPortHostName,controllerPort);
        controllerSocket = new Socket(controllerPortHostName,controllerPort);
        RequestsToController.Enroll enroll = RequestsToController.Enroll.newBuilder()
                .setPort(storageNodePort)
                .setHostname(getHostname())
                .build();
        RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                .newBuilder()
                .setEnrollMsg(enroll).build();

        wrapper.writeDelimitedTo(controllerSocket.getOutputStream());
        logger.info("Waiting for Controller to acknowledge enrollment to start server...");
        ResponsesToStorageNode.AcknowledgeEnrollment response = ResponsesToStorageNode.AcknowledgeEnrollment
                .parseDelimitedFrom(controllerSocket.getInputStream());
        logger.info("Successfully enrolled with Controller!!");
        controllerSocket.close();
        logger.info("Starting Storage Node on port {} with data directory on {}",storageNodePort,dataDirectory);
        if(response.getSuccess())
        {
            ServerSocket serverSocket = new ServerSocket(storageNodePort);
            logger.info("Listening...");
            while (true) {
                TimerTask task = new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            controllerSocket = new Socket(controllerPortHostName,controllerPort);
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

                            wrapper.writeDelimitedTo(controllerSocket.getOutputStream());
                            //controllerSocket.close();
                        }
                        catch (UnknownHostException e)
                        {
                            logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
                        }
                        catch (IOException e)
                        {
                            logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
                        }
                    }
                };
                Timer timer = new Timer();
                long delay = 0;
                long intervalPeriod = 5 * 1000;
                timer.scheduleAtFixedRate(task,delay,intervalPeriod);
                connSocket = serverSocket.accept();
                new Thread(new Request(connSocket)).start();
            }
        }
    }

    public class Request implements Runnable
    {
        Socket connSocket = null;

        public Request(Socket connSocket)
        {
            this.connSocket = connSocket;
        }

        @Override
        public void run()
        {
            try
            {
                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                        .parseDelimitedFrom(connSocket.getInputStream());
                InetAddress inetAddress = connSocket.getInetAddress();
                int port = connSocket.getPort();

                if(requestsWrapper.hasStoreChunkRequestToSNMsg())
                {
                    //Process the Request

                    RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = requestsWrapper.getStoreChunkRequestToSNMsg();
                    String filename = null;
                    int chunkId = 0;

                    byte[] bytes = null;

                    if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromClientMsg())
                    {
                        logger.info("Received Store chunk request from Client {} from port {} ",inetAddress,port);
                        RequestsToStorageNode.StoreChunkRequestToSNFromClient storeChunkRequestToSNFromClient = storeChunkRequestToSN.getStoreChunkRequestToSNFromClientMsg();
                        filename = storeChunkRequestToSNFromClient.getFilename();
                        chunkId  = storeChunkRequestToSNFromClient.getChunkId();
                        bytes =  storeChunkRequestToSNFromClient.getChunkData().toByteArray();
                    }
                    else if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromSNMsg())
                    {
                        logger.info("Received Store chunk request from SN {} from port {} ",inetAddress,port);
                        RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSNFromSN = storeChunkRequestToSN.getStoreChunkRequestToSNFromSNMsg();
                        filename = storeChunkRequestToSNFromSN.getFilename();
                        chunkId  = storeChunkRequestToSNFromSN.getChunkId();
                        bytes =  storeChunkRequestToSNFromSN.getChunkData().toByteArray();
                    }

                    /*Storing Chunk data on local file system of Node*/
                    String blockFile = null;
                    logger.info("filename {} ",filename);
                    if((filename != null) && filename.endsWith(".txt"))
                    {
                        filename = filename.split("\\.")[0];
                        blockFile = dataDirectory + filename + "Part" + chunkId +".txt";
                    }
                    else
                    {
                        blockFile = dataDirectory + filename + "Part" + chunkId;
                    }
                    int i=0;
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

                        wrapper.writeDelimitedTo(connSocket.getOutputStream());
                        logger.info("Store Chunk done and sent a response to client {} to port {} ",inetAddress,port);
                    }
                    if(storeChunkRequestToSN.hasStoreChunkRequestToSNFromSNMsg())
                    {
                        ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN.newBuilder()
                                .setSuccess(true).build();
                        ResponsesToStorageNode.ResponsesToStorageNodeWrapper wrapper = ResponsesToStorageNode.ResponsesToStorageNodeWrapper.newBuilder()
                                .setAcknowledgeStoreChunkToSNMsg(acknowledgeStoreChunkToSN).build();

                        wrapper.writeDelimitedTo(connSocket.getOutputStream());
                        logger.info("Store Chunk done and sent a response to SN {} to port {} ",inetAddress,port);
                    }
                    connSocket.close();
                }

                if(requestsWrapper.hasRetrieveFileRequestToSNMsg())
                {
                    logger.info("Received Retrieve file request from Client {} from port {}",inetAddress,port);
                    RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = requestsWrapper.getRetrieveFileRequestToSNMsg();
                    String filename  = requestToSN.getFilename();
                    byte[] chunkData = null;
                    if((filename != null) && filename.endsWith(".txt"))
                    {
                        filename = filename.split("\\.")[0];
                        chunkData = Files.readAllBytes(new File(dataDirectory + filename +"Part"+requestToSN.getChunkId()+".txt").toPath());
                    }
                    else
                    {
                        chunkData = Files.readAllBytes(new File(dataDirectory + filename +"Part"+requestToSN.getChunkId()).toPath());
                    }

                    logger.info("chunk data {}",chunkData);
                    ResponsesToClient.RetrieveFileResponseFromSN response = ResponsesToClient.RetrieveFileResponseFromSN.newBuilder()
                            .setChecksum(0)
                            .setFilename(requestToSN.getFilename())
                            .setChunkId(requestToSN.getChunkId())
                            .setChunkData(ByteString.copyFrom(chunkData)).build();
                    response.writeDelimitedTo(connSocket.getOutputStream());
                    logger.info("Retrieve chunk from this node is done and sent back response");
                    connSocket.close();
                }

                if(requestsWrapper.hasReadinessCheckRequestToSNMsg())
                {
                    RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN = requestsWrapper.getReadinessCheckRequestToSNMsg();
                    if(readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromClientMsg())
                    {
                        logger.info("Received readiness check request from client {} from port {} ",inetAddress,port);
                        ResponsesToClient.AcknowledgeReadinessToClient readinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.newBuilder()
                                .setSuccess(true).build();
                        readinessToClient.writeDelimitedTo(connSocket.getOutputStream());
                        logger.info("Sent acknowledgement for readiness check to client");
                        logger.info("list of nodes {}",readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList());
                        if(readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList().size() > 0) {
                            logger.info("Another Thread which sends request to other storage nodes in list ");
                            new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)).start();
                        }
                    }

                    if(readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromSNMsg())
                    {
                        logger.info("Received readiness check request from Storage Node {} from port {} ",inetAddress, port);
                        ResponsesToStorageNode.AcknowledgeReadinessToSN readinessToClient = ResponsesToStorageNode.AcknowledgeReadinessToSN.newBuilder()
                                .setSuccess(true).build();
                        readinessToClient.writeDelimitedTo(connSocket.getOutputStream());
                        logger.info("Sent acknowledgement for readiness check to Storage Node");
                        logger.info("list of nodes {}",readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList());
                        if(readinessCheckRequestToSN.getReadinessCheckRequestToSNFromSNMsg().getStorageNodeListList().size() > 0) {
                            logger.info("Another Thread which sends request to other storage nodes in list ");
                            new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)).start();
                        }
                    }

                }

            }
            catch (NoSuchAlgorithmException e)
            {
                logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
            }
            catch (IOException e)
            {
                logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
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

                    if (requestToSNFromClient.getStorageNodeListList().size() > 0)
                    {
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode> peerList = requestToSNFromClient.getStorageNodeListList();
                        String filename = requestToSNFromClient.getFilename();
                        int chunkId = requestToSNFromClient.getChunkId();

                        String hostname = peerList.get(0).getHostname();
                        if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                        {
                            hostname = "Bhargavis-MacBook-Pro.local";
                        }
                        Socket socket = new Socket(hostname, peerList.get(0).getPort());
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
                                .addAllStorageNodeList(peers)
                                .setFilename(filename)
                                .setChunkId(chunkId);
                        RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                .setReadinessCheckRequestToSNFromSNMsg(builder1);

                        RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                .setReadinessCheckRequestToSNMsg(builder).build();
                        logger.info("Sending readiness check request to peer SN {} to port {} ",hostname,socket.getPort());
                        requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                        logger.info("Waiting for response from peer SN {} from port {}",hostname,socket.getPort());
                        ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                                .parseDelimitedFrom(socket.getInputStream());
                        logger.info("Received response from peer SN {} from port {}",hostname,socket.getPort());
                        socket.close();
                        String filepath = null;
                        if((filename != null) && filename.endsWith(".txt"))
                        {
                            filename = filename.split("\\.")[0];
                            filepath = dataDirectory + filename + "Part" + chunkId + ".txt";
                            filename = filename + ".txt";
                        }
                        else
                        {
                            filepath = dataDirectory + filename + "Part" + chunkId;
                        }
                        if (acknowledgeReadinessToSN.getSuccess()) {
                            Socket socket1 = new Socket(hostname, peerList.get(0).getPort());
                            File file = new File(filepath);
                            while(!file.exists())
                            {
                                Thread.sleep(1000);
                            }
                            logger.info("Store chunk request to peer SN {} to port {} ",hostname,socket1.getPort());
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
                            wrapper.writeDelimitedTo(socket1.getOutputStream());
                            ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN
                                    .parseDelimitedFrom(socket1.getInputStream());
                            logger.info("Received store chunk response from peer SN {} from port {} ",hostname,socket1.getPort());
                            if (acknowledgeStoreChunkToSN.getSuccess())
                            {
                                logger.info("Store chunk..success in peer SN {} from port {} ",hostname,socket1.getPort());
                            }
                            socket1.close();
                        }
                    }
                }

                if(readinessCheckRequestToSNMsg.hasReadinessCheckRequestToSNFromSNMsg())
                {
                    RequestsToStorageNode.ReadinessCheckRequestToSNFromSN requestToSNFromSN = readinessCheckRequestToSNMsg.getReadinessCheckRequestToSNFromSNMsg();

                    if (requestToSNFromSN.getStorageNodeListList().size() > 0)
                    {
                        List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peerList = requestToSNFromSN.getStorageNodeListList();
                        String filename = requestToSNFromSN.getFilename();
                        int chunkId = requestToSNFromSN.getChunkId();

                        String hostname = peerList.get(0).getHostname();
                        if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                        {
                            hostname = "Bhargavis-MacBook-Pro.local";
                        }
                        Socket socket = new Socket(hostname, peerList.get(0).getPort());
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
                                .setFilename(filename)
                                .setChunkId(chunkId)
                                .addAllStorageNodeList(peers);
                        RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                .setReadinessCheckRequestToSNFromSNMsg(builder1);

                        RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                .setReadinessCheckRequestToSNMsg(builder).build();
                        logger.info("Sending readiness check(SN-SN) request to peer SN {} to port {} ",hostname,socket.getPort());
                        requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                        logger.info("Waiting for readiness response from peer SN {} from port {}",hostname,socket.getPort());
                        ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                                .parseDelimitedFrom(socket.getInputStream());
                        logger.info("Received readiness response(SN-SN) from peer SN {} from port {}",hostname,socket.getPort());
                        socket.close();
                        String filepath = null;
                        if((filename != null) && filename.endsWith(".txt"))
                        {
                            filename = filename.split("\\.")[0];
                            filepath = dataDirectory + filename + "Part" + chunkId + ".txt";
                            filename = filename + ".txt";
                        }
                        else
                        {
                            filepath = dataDirectory + filename + "Part" + chunkId;
                        }
                        if (acknowledgeReadinessToSN.getSuccess()) {
                            Socket socket1 = new Socket(hostname,peerList.get(0).getPort());
                            File file = new File(filepath);
                            while(!file.exists())
                            {
                                logger.debug("inside while sleeping till file is present looking for file {}",filepath);
                                Thread.sleep(1000);
                            }
                            logger.info("Store chunk request(SN-SN) to peer SN {} to port {} ",hostname,socket1.getPort());
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
                            wrapper.writeDelimitedTo(socket1.getOutputStream());
                            ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN
                                    .parseDelimitedFrom(socket1.getInputStream());
                            logger.info("Received store chunk(SN-SN) response from peer SN {} from port {}",hostname,socket1.getPort());
                            if (acknowledgeStoreChunkToSN.getSuccess())
                            {
                                logger.info("Store chunk..success in peer SN {} from port {}",hostname,socket1.getPort());
                            }
                            socket1.close();
                        }
                    }
                }
            }
            catch (IOException e)
            {
                logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
            }
            catch (InterruptedException e)
            {
                logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
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

package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class StorageNode {

    public static final Logger logger = LoggerFactory.getLogger(StorageNode.class);
    private int controllerPort;
    private String controllerPortHostName;
    private int storageNodePort;
    private Map<String,StorageNodeMetadata> storageNodeMetadataMap = new HashMap<>();
    private Map<String,StorageNodeMetadata> dataStoredInLastFiveSeconds = new HashMap<>();
    private Socket controllerSocket = null;
    private Socket connSocket = null;
    public static final int NUM_THREADS_ALLOWED = 15;
    private ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);
    private String dataDirectory ;

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

        if(args.length == 4)
        {
            if (args[0] != null)
            {
                controllerPortHostName = args[0];
            }
            System.out.println(controllerPortHostName);
            if (args[1] != null)
            {
                controllerPort = Integer.parseInt(args[1]);
                System.out.println(controllerPort);
            }
            if(args[2] != null)
            {
                storageNodePort = Integer.parseInt(args[2]);
                System.out.println(controllerPortHostName);
            }
            if(args[3] != null)
            {
                dataDirectory = args[3];
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
            executorService.submit(new Thread(new HeartBeat()));
            while (true) {
                connSocket = serverSocket.accept();
                executorService.submit(new Thread(new Request(connSocket)));
            }
        }
    }

    public class HeartBeat implements Runnable
    {
        @Override
        public void run()
        {
            while (true)
            {
                try
                {
                    logger.debug("heart beat");
                    controllerSocket = new Socket(controllerPortHostName, controllerPort);
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
                    controllerSocket.close();
                    Thread.sleep(5000);
                }
                catch (UnknownHostException e)
                {
                    logger.error("exception caught {}",ExceptionUtils.getStackTrace(e));
                }
                catch (IOException e)
                {
                    logger.error("exception caught {}",ExceptionUtils.getStackTrace(e));
                }
                catch (InterruptedException e)
                {
                    logger.error("exception caught {}",ExceptionUtils.getStackTrace(e));
                }
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
                    blockFile = dataDirectory + filename + "Part" + chunkId;
                    int i=0;
                    Files.deleteIfExists(Paths.get(blockFile));
                    Files.createFile(Paths.get(blockFile));
                    Files.write(Paths.get(blockFile),bytes);


                    /*Calculating Checksum and adding all the chunkInfo(filename,chunkId,Checksum) to metadata Map of Storage Node*/
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(blockFile)));
                    StringBuilder checksum = new StringBuilder();
                    for (int j = 0; j < mdbytes.length; ++j)
                    {
                        checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
                    }
                    logger.info("checksum {}",checksum.toString());
                    StorageNodeMetadata metadata = new StorageNodeMetadata(filename,chunkId);
                    metadata.setChecksum(checksum.toString());
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

                    chunkData = Files.readAllBytes(new File(dataDirectory + filename +"Part"+requestToSN.getChunkId()).toPath());
                    StorageNodeMetadata storageNodeMetadata = storageNodeMetadataMap.get(requestToSN.getFilename()+requestToSN.getChunkId());
                    logger.debug("chunk data {}",chunkData);
                    ResponsesToClient.RetrieveFileResponseFromSN response = ResponsesToClient.RetrieveFileResponseFromSN.newBuilder()
                            .setChecksum(storageNodeMetadata.getChecksum())
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
                            executorService.submit(new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)));
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
                            executorService.submit(new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)));
                        }
                    }

                }
                if(requestsWrapper.hasSendGoodChunkRequestToSNMsg())
                {
                    logger.info("Received good chunk request from Client!!");

                    RequestsToStorageNode.SendGoodChunkRequestToSN sn = requestsWrapper.getSendGoodChunkRequestToSNMsg();
                    String f = dataDirectory+sn.getFilename()+"Part"+sn.getChunkId();
                    logger.debug("corrupted data {}",Files.readAllBytes(Paths.get(f)));
                    RequestsToController.SendGoodChunkRequest.storageNode storageNode = RequestsToController.SendGoodChunkRequest.storageNode.newBuilder()
                                                                                        .setHostname(getHostname())
                                                                                        .setPort(storageNodePort).build();
                    //request to controller to send address of host which has same replica other than this
                    RequestsToController.SendGoodChunkRequest sendGoodChunkRequest = RequestsToController.SendGoodChunkRequest.newBuilder()
                                                                                    .setFilename(sn.getFilename())
                                                                                    .setChunkId(sn.getChunkId())
                                                                                    .setSN(storageNode).build();
                    RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                                                                                .setSendGoodChunkRequestMsg(sendGoodChunkRequest).build();
                    Socket socket = new Socket(controllerPortHostName,controllerPort);
                    logger.info("Sending good chunk request to CN....");
                    wrapper.writeDelimitedTo(socket.getOutputStream());
                    ResponsesToStorageNode.GoodChunkInfoToSN goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN
                                                                                .parseDelimitedFrom(socket.getInputStream());
                    logger.info("Received good chunk request from CN!!");
                    socket.close();
                    logger.info("good chunk is in host {} port {}",goodChunkInfoToSN.getSN().getHostname(),goodChunkInfoToSN.getSN().getPort());
                    socket = new Socket(goodChunkInfoToSN.getSN().getHostname(),goodChunkInfoToSN.getSN().getPort());
                    RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.storageNode SN = RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.storageNode
                                                                                            .newBuilder()
                                                                                            .setHostname(getHostname())
                                                                                            .setPort(storageNodePort).build();
                    RequestsToStorageNode.SendGoodChunkRequestFromSNToSN snToSN = RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.newBuilder()
                                                                                    .setSN(SN)
                                                                                    .setFilename(goodChunkInfoToSN.getFilename())
                                                                                    .setChunkId(goodChunkInfoToSN.getChunkId()).build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper1 = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                    .setSendGoodChunkRequestFromSNToSNMsg(snToSN).build();
                    logger.info("Sending good chunk request to SN {} from port {}",goodChunkInfoToSN.getSN().getHostname(),goodChunkInfoToSN.getSN().getPort());
                    wrapper1.writeDelimitedTo(socket.getOutputStream());
                    ResponsesToStorageNode.GoodChunkDataToSN chunkDataToSN = ResponsesToStorageNode.GoodChunkDataToSN
                                                                            .parseDelimitedFrom(socket.getInputStream());
                    logger.info("Received good chunk data from SN ");
                    socket.close();


                    //replace the chunk
                    String key = chunkDataToSN.getFilename()+ "Part" + chunkDataToSN.getChunkId();
                    String filePath = dataDirectory+key;
                    File file = new File(filePath);
                    byte[] chunkData = chunkDataToSN.getChunkData().toByteArray();
                    logger.info("chunkdata {}",chunkData);
                    if(file.exists())
                    {
                        boolean check = file.delete();
                        if(check)
                        {
                            file.createNewFile();
                            Files.write(Paths.get(filePath),chunkData);
                        }
                    }
                    //replace checksum in the entry of metadata
                    MessageDigest md = MessageDigest.getInstance("MD5");

                    byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(filePath)));
                    StringBuilder checksum = new StringBuilder();
                    for (int j = 0; j < mdbytes.length; ++j)
                    {
                        checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
                    }
                    logger.info("checksum {}",checksum.toString());
                    StorageNodeMetadata metadata = new StorageNodeMetadata(chunkDataToSN.getFilename(),chunkDataToSN.getChunkId());
                    metadata.setChecksum(checksum.toString());
                    storageNodeMetadataMap.put(chunkDataToSN.getFilename()+chunkDataToSN.getChunkId(),metadata);
                    logger.info("good chunkData {}",Files.readAllBytes(Paths.get(filePath)));
                    //send good chunk back to client which is waiting
                    BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
                    String line = null;
                    while ((line = reader.readLine()) != null)
                    {
                        logger.debug("file info {}",line);
                    }
                    ResponsesToClient.GoodChunkDataToClient goodChunkData = ResponsesToClient.GoodChunkDataToClient.newBuilder()
                                                                            .setChunkData(ByteString.copyFrom(Files.readAllBytes(Paths.get(filePath))))
                                                                            .setChecksum(checksum.toString())
                                                                            .setFilename(chunkDataToSN.getFilename())
                                                                            .setChunkId(chunkDataToSN.getChunkId()).build();
                    logger.info("Sending good chunk data back to client!!!");
                    goodChunkData.writeDelimitedTo(connSocket.getOutputStream());
                    connSocket.close();
                }
                if(requestsWrapper.hasSendGoodChunkRequestFromSNToSNMsg())
                {
                    RequestsToStorageNode.SendGoodChunkRequestFromSNToSN SN = requestsWrapper.getSendGoodChunkRequestFromSNToSNMsg();
                    logger.info("Received good chunk data req from peer SN {} from port {} for file {} chunk {}",SN.getSN().getHostname()
                            ,SN.getSN().getPort(),SN.getFilename(),SN.getChunkId());
                    String filePath = dataDirectory + SN.getFilename()+"Part"+ SN.getChunkId();
                    byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
                    logger.info("chunkData {}",chunkData);
                    ResponsesToStorageNode.GoodChunkDataToSN chunkDataToSN = ResponsesToStorageNode.GoodChunkDataToSN.newBuilder()
                                                                            .setChunkData(ByteString.copyFrom(chunkData))
                                                                            .setFilename(SN.getFilename())
                                                                            .setChunkId(SN.getChunkId()).build();
                    chunkDataToSN.writeDelimitedTo(connSocket.getOutputStream());
                    logger.info("Sending response with good chunk data..");
                    connSocket.close();
                }
                if(requestsWrapper.hasSendReplicaCopyToSNMsg())
                {
                    RequestsToStorageNode.SendReplicaCopyToSN replicaCopyToSN = requestsWrapper.getSendReplicaCopyToSNMsg();
                    logger.info("Received send replica copy request from controller...");

                    //get that file chunk data based on sent info

                    String filePath = dataDirectory + replicaCopyToSN.getFilename()+ "Part" + replicaCopyToSN.getChunkId();
                    byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
                    RequestsToStorageNode.SendReplicaCopyToSNFromSN replicaCopyToSNFromSN = RequestsToStorageNode.SendReplicaCopyToSNFromSN.newBuilder()
                                                                                            .setChunkData(ByteString.copyFrom(chunkData))
                                                                                            .setFilename(replicaCopyToSN.getFilename())
                                                                                            .setChunkId(replicaCopyToSN.getChunkId()).build();
                    Socket socket = new Socket(replicaCopyToSN.getSN().getHostname(),replicaCopyToSN.getSN().getPort());
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                .setSendReplicaCopyToSNFromSNMsg(replicaCopyToSNFromSN).build();
                    logger.info("Sent replica copy to SN hostname {} port {}",replicaCopyToSN.getSN().getHostname(),replicaCopyToSN.getSN().getPort());
                    wrapper.writeDelimitedTo(socket.getOutputStream());
                }
                if(requestsWrapper.hasSendReplicaCopyToSNFromSNMsg())
                {
                    RequestsToStorageNode.SendReplicaCopyToSNFromSN replicaCopyToSNFromSN = requestsWrapper.getSendReplicaCopyToSNFromSNMsg();
                    logger.info("Received replica store request from SN..");
                    String fileName = replicaCopyToSNFromSN.getFilename();
                    int chunkId = replicaCopyToSNFromSN.getChunkId();
                    String filePath = dataDirectory + fileName +"Part"+ chunkId;
                    Files.createFile(Paths.get(filePath));
                    Files.write(Paths.get(filePath),replicaCopyToSNFromSN.getChunkData().toByteArray());

                    //checksum
                    MessageDigest md = MessageDigest.getInstance("MD5");

                    byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(filePath)));
                    StringBuilder checksum = new StringBuilder();
                    for (int j = 0; j < mdbytes.length; ++j)
                    {
                        checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
                    }
                    logger.info("checksum {}",checksum.toString());
                    StorageNodeMetadata metadata = new StorageNodeMetadata(fileName,chunkId);
                    metadata.setChecksum(checksum.toString());
                    storageNodeMetadataMap.put(fileName+chunkId,metadata);
                    logger.info("Stored replica on my storage node..");
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

                        filepath = dataDirectory + filename + "Part" + chunkId;

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

                        filepath = dataDirectory + filename + "Part" + chunkId;
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

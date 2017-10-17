package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static SortedMap<Integer,byte[]> listOfChunks;
    private static final int CHUNK_SIZE = 1000000;
    public static final int NUM_THREADS_ALLOWED = 15;
    private static ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);

    public static void main(String[] args) throws Exception{

        String controllerHostname = args[0];
        int controllerPort = Integer.parseInt(args[1]);


        if(args[2].equals("store")) {

            //chunking functionality
            String filePath = args[3];
            int filePart = 1;
            List<byte[]> blocks = chunking(filePath);

            for (byte[] block : blocks) {
                //sending block to Controller with blockInfo
                //StoreChunk request to Controller
                logger.info("Controller hostname {} Controller port {} ",controllerHostname,controllerPort);
                Socket socket = new Socket(controllerHostname, controllerPort);
                String filename = args[3];
                String[] tokens = filename.split("/");
                filename = tokens[tokens.length - 1];
                RequestsToController.StoreChunkRequest storeChunk
                        = RequestsToController.StoreChunkRequest.newBuilder()
                        .setChunkId(filePart)
                        .setFilename(filename).build();
                RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                        .setStoreChunkRequestMsg(storeChunk).build();
                logger.info("Sending StoreChunk request to Controller {} to port {}",controllerHostname,controllerPort);
                requestsToControllerWrapper.writeDelimitedTo(socket.getOutputStream());

                logger.info("Waiting for StoreChunk response from Controller...");

                //Received response from Controller with list of three Storage Nodes to store the replicas
                ResponsesToClient.StoreChunkResponse response = ResponsesToClient.StoreChunkResponse.parseDelimitedFrom(socket.getInputStream());

                logger.info("Received StoreChunk response from Controller...");
                socket.close();

                //ReadinessCheck request to Storage Node-1
                String hostname = response.getStorageNodeList(0).getHostname();
                if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                {
                    hostname = "Bhargavis-MacBook-Pro.local";
                }
                Socket socket1 = new Socket(hostname, response.getStorageNodeList(0).getPort());

                List<RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode> storageNodeList = new ArrayList<>();
                for(int i=1;i<response.getStorageNodeListList().size();i++)
                {

                    RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode readinessCheck = RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeListList().get(i).getPort()).setHostname(response.getStorageNodeListList().get(i).getHostname()).build();
                    storageNodeList.add(readinessCheck);
                }
                RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.newBuilder()
                                                                                    .setFilename(filename)
                                                                                    .setChunkId(filePart)
										    .addAllStorageNodeList(storageNodeList);

                RequestsToStorageNode.ReadinessCheckRequestToSN requestToSN = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                                                                .setReadinessCheckRequestToSNFromClientMsg(builder).build();

                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setReadinessCheckRequestToSNMsg(requestToSN).build();


                logger.info("Sending readinessCheck request to Storage Node {} to port {}",socket1.getInetAddress(),socket1.getPort());
                requestsToStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());

                //Received response from Storage Node-1 regarding Readiness Check
                ResponsesToClient.AcknowledgeReadinessToClient acknowledgeReadinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.parseDelimitedFrom(socket1.getInputStream());

                logger.info("Received readinessCheck response from Storage Node...");

                socket1.close();
                if (acknowledgeReadinessToClient.getSuccess()) {
                    //sends chunkMetadata and data to Storage Nodes in pipeline fashion for storage
                    //StoreChunkRequest to Storage Node
                    hostname = response.getStorageNodeList(0).getHostname();
                    if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                    {
                        hostname = "Bhargavis-MacBook-Pro.local";
                    }
                    Socket socket2 = new Socket(hostname,response.getStorageNodeList(0).getPort());
                    RequestsToStorageNode.StoreChunkRequestToSNFromClient.StorageNode storageNode = RequestsToStorageNode.StoreChunkRequestToSNFromClient.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeList(0).getPort()).build();


                    RequestsToStorageNode.StoreChunkRequestToSNFromClient storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromClient.newBuilder()
                            .addStorageNodeList(storageNode)
                            .setChunkId(filePart)
                            .setFilename(filename)
                            .setChunkData(ByteString.copyFrom(block)).build();

                    RequestsToStorageNode.StoreChunkRequestToSN chunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                                                                    .setStoreChunkRequestToSNFromClientMsg(storeChunkRequestToSN).build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                            .setStoreChunkRequestToSNMsg(chunkRequestToSN).build();

                    logger.info("Sending store chunk request to Storage Node {} to port {} ",socket2.getInetAddress(),socket2.getPort());
                    wrapper.writeDelimitedTo(socket2.getOutputStream());
                    logger.info("Waiting for store chunk response from Storage Node...");

                    ResponsesToClient.ResponsesToClientWrapper responsesToClientWrapper = ResponsesToClient.ResponsesToClientWrapper.parseDelimitedFrom(socket2.getInputStream());

                    if (responsesToClientWrapper.hasAcknowledgeStoreChunkToClientMsg()) {
                        if (responsesToClientWrapper.getAcknowledgeStoreChunkToClientMsg().getSuccess())
                            logger.info("Received response from Storage Node!!success");
                        else
                            logger.info("Received response from Storage Node!!fail");
                    }
                    socket2.close();
                }
                filePart = filePart + 1;
            }
        }


        else if(args[2].equals("retrieve"))
        {
            //RetrieveFileRequest to Controller
            listOfChunks = new TreeMap<>();
            String currPath = ".";
            Path p = Paths.get(currPath);
            Path absDir = p.toAbsolutePath();
            String fileRequired = args[3];
            String[] tokens = fileRequired.split("/");
            int length = tokens.length;
            String filename = null;
            String mergedFile = null;
            if(length > 0)
            {
                filename = tokens[length-1];
                if(filename.contains(".txt"))
                {
                    filename = tokens[length - 1].split("\\.")[0];
                    mergedFile = absDir.toString() + "/retrievedFilesDirectory/"+filename + ".txt";
                }
                else
                {
                    mergedFile = absDir.toString() + "/retrievedFilesDirectory/"+filename;
                }

            }


            RequestsToController.RetrieveFileRequest retrieveFileRequest = RequestsToController.RetrieveFileRequest.newBuilder()
                    .setFilename(filename)
                    .build();
            RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper1 = RequestsToController.RequestsToControllerWrapper.newBuilder().setRetrieveFileRequestMsg(retrieveFileRequest).build();
            logger.info("Sending RetrieveFile request to Controller {} to port {}",controllerHostname,controllerPort);
            Socket socket = new Socket(controllerHostname, controllerPort);
            requestsToControllerWrapper1.writeDelimitedTo(socket.getOutputStream());

            //Response from Controller with Storage Nodes list which host the replicas of chunks of given file
            logger.info("Waiting for RetrieveFile response from Controller...");
            ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received RetrieveFile response from Controller...");
            socket.close();

            for (ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata : responseFromCN.getChunkListList()) {

                Thread thread = new Thread(new ChunkRetrieveWorker(chunkMetadata));
                executorService.submit(thread);
            }
            executorService.shutdown();
            try
            {
                executorService.awaitTermination(40, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                logger.error("Exception caught {}",ExceptionUtils.getStackTrace(e));
            }
            logger.info("byte array size {}",listOfChunks.size());
            Files.createFile(Paths.get(mergedFile));
            for(int key : listOfChunks.keySet())
            {
                byte[] temp = listOfChunks.get(key);
                Files.write(Paths.get(mergedFile),temp, StandardOpenOption.APPEND);
            }
            logger.info("checksum of retrieved file {}",calculateChecksum(Files.readAllBytes(Paths.get(mergedFile))));
        }
        else if(args[2].equals("list"))
        {
            Socket socket2 = new Socket(controllerHostname,controllerPort);
            RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest = RequestsToController.ListOfActiveNodesRequest
                                                                                        .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                                                                        .newBuilder()
                                                                        .setListOfActiveNodes(listOfActiveNodesRequest)
                                                                        .build();
            logger.info("Sending  a list request to Controller...");
            wrapper.writeDelimitedTo(socket2.getOutputStream());
            ResponsesToClient.ListOfActiveStorageNodesResponseFromCN list = ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.parseDelimitedFrom(socket2.getInputStream());
            logger.info("Received response from controller");
            for(ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo info : list.getActiveStorageNodesList())
            {
                logger.info("Filename: {} Host: {} Port: {}",info.getFilename(),info.getHostname(),info.getPort());
            }
        }
    }

    public static class ChunkRetrieveWorker implements Runnable
    {
        ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata;
        public ChunkRetrieveWorker(ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata ) {
            this.chunkMetadata = chunkMetadata;
        }

        @Override
        public void run() {
            try
            {
                RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = RequestsToStorageNode.RetrieveFileRequestToSN.newBuilder()
                        .setChunkId(chunkMetadata.getChunkId())
                        .setFilename(chunkMetadata.getFilename())
                        .build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper toStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setRetrieveFileRequestToSNMsg(requestToSN).build();
                Socket socket1 = new Socket(chunkMetadata.getNode().getHostname(), chunkMetadata.getNode().getPort());
                logger.info("Sending RetrieveFile request to Storage Node {} to port {}", socket1.getInetAddress(), socket1.getPort());
                logger.info("Sending request for {} chunk {}",chunkMetadata.getFilename(),chunkMetadata.getChunkId());
                toStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());
                logger.info("Waiting for RetrieveFile response from Storage Node...");


                ResponsesToClient.RetrieveFileResponseFromSN responseFromSN = ResponsesToClient.RetrieveFileResponseFromSN.parseDelimitedFrom(socket1.getInputStream());
                logger.info("Received RetrieveFile response from Storage Node...");
                byte[] temp = responseFromSN.getChunkData().toByteArray();
                String checksum = calculateChecksum(temp);
                String checksumDesired = responseFromSN.getChecksum();
                logger.info("checksum {} checksum desired {}",checksum,checksumDesired);

                //check the checksum
                while (!checksumDesired.equals(checksum))
                {
                    socket1.close();
                    //checksum does not match
                    //send request for right copy
                    RequestsToStorageNode.SendGoodChunkRequestToSN goodChunkRequestToSN = RequestsToStorageNode.SendGoodChunkRequestToSN
                                                                                           .newBuilder()
                                                                                            .setFilename(chunkMetadata.getFilename())
                                                                                            .setChunkId(chunkMetadata.getChunkId())
                                                                                            .build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                .setSendGoodChunkRequestToSNMsg(goodChunkRequestToSN).build();
                    socket1 = new Socket(chunkMetadata.getNode().getHostname(),chunkMetadata.getNode().getPort());
                    logger.info("Sending good chunk request to SN {} to port {}",chunkMetadata.getNode().getHostname(),chunkMetadata.getNode().getPort());
                    wrapper.writeDelimitedTo(socket1.getOutputStream());
                    logger.info("Waiting for good chunk response from SN...");
                    ResponsesToClient.GoodChunkDataToClient goodChunkData = ResponsesToClient.GoodChunkDataToClient
                                                                            .parseDelimitedFrom(socket1.getInputStream());
                    logger.info("Received good chunk data from SN {} from port {}",chunkMetadata.getNode().getHostname(),chunkMetadata.getNode().getPort());
                    temp = goodChunkData.getChunkData().toByteArray();
                    logger.info("chunkdata {}",temp);
                    checksum = calculateChecksum(temp);
                    checksumDesired = goodChunkData.getChecksum();
                    logger.info("checksumDesired {} checksum {}",checksumDesired,checksum);
                }
                listOfChunks.put(responseFromSN.getChunkId(),temp);
                System.out.println("list of chunks {}"+ listOfChunks);
                socket1.close();
            }
            catch (IOException e)
            {
                logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private static String calculateChecksum(byte[] bytes)
    {
        StringBuilder checksum = new StringBuilder();
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] mdbytes = md.digest(bytes);
            for (int j = 0; j < mdbytes.length; ++j) {
                checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
            }

        }
        catch (NoSuchAlgorithmException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
        return checksum.toString();
    }

    private static List chunking(String filePath) throws Exception
    {
        int i=0;

        byte[] bFile = Files.readAllBytes(new File(filePath).toPath());
        int fileSize = bFile.length;
        logger.info("fileSize in bytes {}",fileSize);

        int numBlocks  = (fileSize / CHUNK_SIZE) ;
        if((fileSize % CHUNK_SIZE) != 0)
            numBlocks = numBlocks + 1;
        logger.info("number of blocks {}", numBlocks);
        List<byte[]> blocks = new ArrayList<>();
        ByteArrayOutputStream bos = null;

        while(i < fileSize)
        {
            bos = new ByteArrayOutputStream();
            for (int j = 0; j < CHUNK_SIZE; j++)
            {
                if(i<fileSize)
                {
                    bos.write(bFile[i]);
                    i++;
                }
            }
            blocks.add(bos.toByteArray());
        }

        return blocks;
    }

}

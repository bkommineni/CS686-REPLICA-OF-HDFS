package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    
    public static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static List<byte[]> listOfChunks = new ArrayList<>();

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
            String currPath = ".";
            Path p = Paths.get(currPath);
            Path absDir = p.toAbsolutePath();
            String fileRequired = args[3];
            String[] tokens = fileRequired.split("/");
            int length = tokens.length;
            String filename  = tokens[length-1].split("\\.")[0];
            String mergedFile = absDir.toString() + "/retrievedFilesDirectory/"+filename + ".txt";
            RequestsToController.RetrieveFileRequest retrieveFileRequest = RequestsToController.RetrieveFileRequest.newBuilder()
                    .setFilename(filename+".txt")
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

            //List<Thread> threads = new ArrayList<>();

            for (ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata : responseFromCN.getChunkListList()) {
                Thread thread = new Thread(new ChunkRetrieveWorker(chunkMetadata));
                thread.start();
                thread.join();
            }

            FileWriter writer = new FileWriter(mergedFile);
            logger.info("byte array size {}",listOfChunks.size());
            for(int i=0;i<listOfChunks.size();i++)
            {
                byte[] temp = listOfChunks.get(i);
                int k = 0;
                while (k < temp.length) {
                    writer.write(temp[k]);
                    k++;
                }
            }
            writer.close();
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
            wrapper.writeDelimitedTo(socket2.getOutputStream());
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
                toStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());
                logger.info("Waiting for RetrieveFile response from Storage Node...");


                ResponsesToClient.RetrieveFileResponseFromSN responseFromSN = ResponsesToClient.RetrieveFileResponseFromSN.parseDelimitedFrom(socket1.getInputStream());
                logger.info("Received RetrieveFile response from Storage Node...");
                byte[] temp = responseFromSN.getChunkData().toByteArray();
                listOfChunks.add(temp);
                System.out.println("list of chunks {}"+ listOfChunks);
                socket1.close();
            }
            catch (IOException e)
            {
                logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
            }
        }
    }

    private static List chunking(String filepath) throws Exception
    {
        int i=0;
        int chunkSize = 2000;
        byte[] bFile = Files.readAllBytes(new File(filepath).toPath());
        int fileSize = bFile.length;

        int numBlocks  = (fileSize / chunkSize) ;
        if((fileSize % chunkSize) != 0)
            numBlocks = numBlocks + 1;
        logger.info("number of blocks {}", numBlocks);
        List<byte[]> blocks = new ArrayList<>();

        while(i < fileSize)
        {
            byte[] block = new byte[chunkSize];
            for (int j = 0; j < chunkSize; j++)
            {
                if(i<fileSize)
                {
                    block[j] = bFile[i];
                    i++;
                }
            }
            blocks.add(block);
        }

        return blocks;
    }

}

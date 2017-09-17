package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Client {

    public static void main(String[] args) throws Exception{


        Socket socket = null;
        Socket socket1 = null;
        int controllerPort = 9998;
        String controllerHostname = InetAddress.getLocalHost().getHostName();

        if(args[0].equals("store")) {

            //chunking functionality
            /*String currPath = ".";
            Path p = Paths.get(currPath);
            Path absDir = p.toAbsolutePath();
            String filePath = absDir.toString() + "/clientDirectory/File1.txt";*/
            System.out.println("storeeee");
            String filePath = args[1];
            int filePart = 1;
            List<byte[]> blocks = chunking(filePath);

            for (byte[] block : blocks) {
                //sending block to Controller with blockInfo
                //StoreChunk request to Controller
                socket = new Socket(controllerHostname, controllerPort);
                System.out.println("controller hostname"+controllerHostname);
                /*String[] tokens = filePath.split("/");
                int noOfTokens = tokens.length;
                String filename = tokens[noOfTokens-1].split(".")[0];*/
                String filename = args[1];
                RequestsToController.StoreChunkRequest storeChunk
                        = RequestsToController.StoreChunkRequest.newBuilder()
                        .setChunkId(filePart)
                        .setFilename(filename).build();
                RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                        .setStoreChunkRequestMsg(storeChunk).build();
                System.out.println("Sending StoreChunk request to Controller...");
                requestsToControllerWrapper.writeDelimitedTo(socket.getOutputStream());

                System.out.println("Waiting for StoreChunk response from Controller...");

                //Received response from Controller with list of three Storage Nodes to store the replicas
                ResponsesToClient.StoreChunkResponse response = ResponsesToClient.StoreChunkResponse.parseDelimitedFrom(socket.getInputStream());

                System.out.println("Received StoreChunk response from Controller...");
                socket.close();

                //ReadinessCheck request to Storage Node-1
                socket1 = new Socket(response.getStorageNodeList(0).getHostname(), response.getStorageNodeList(0).getPort());

                List<RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode> storageNodeList = new ArrayList<>();
                for(int i=1;i<response.getStorageNodeListList().size();i++)
                {

                    RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode readinessCheck = RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeListList().get(i).getPort()).setHostname(response.getStorageNodeListList().get(i).getHostname()).build();
                    storageNodeList.add(readinessCheck);
                }
                RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                                                                    .setFilename(filename)
                                                                                    .setChunkId(filePart);

                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setReadinessCheckRequestToSNMsg(builder).build();


                System.out.println("Sending readinessCheck request to Storage Node...");
                requestsToStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());

                //Received response from Storage Node-1 regarding Readiness Check
                ResponsesToClient.AcknowledgeReadinessToClient acknowledgeReadinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.parseDelimitedFrom(socket1.getInputStream());

                System.out.println("Received readinessCheck response from Storage Node...");


                if (acknowledgeReadinessToClient.getSuccess()) {
                    //sends chunkMetadata and data to Storage Nodes in pipeline fashion for storage
                    //StoreChunkRequest to Storage Node
                    RequestsToStorageNode.StoreChunkRequestToSN.StorageNode storageNode = RequestsToStorageNode.StoreChunkRequestToSN.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeList(0).getPort()).build();


                    RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                            .addStorageNodeList(storageNode)
                            .setChunkId(filePart)
                            .setFilename(filename)
                            .setChunkData(ByteString.copyFrom(block)).build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                            .setStoreChunkRequestToSNMsg(storeChunkRequestToSN).build();

                    System.out.println("Sending store chunk request to Storage Node...");
                    wrapper.writeDelimitedTo(socket1.getOutputStream());
                    System.out.println("Waiting for store chunk response from Storage Node...");

                    ResponsesToClient.ResponsesToClientWrapper responsesToClientWrapper = ResponsesToClient.ResponsesToClientWrapper.parseDelimitedFrom(socket1.getInputStream());

                    if (responsesToClientWrapper.hasAcknowledgeStoreChunkToClientMsg()) {
                        if (responsesToClientWrapper.getAcknowledgeStoreChunkToClientMsg().getSuccess())
                            System.out.println("Received response from Storage Node!!success");
                        else
                            System.out.println("Received response from Storage Node!!fail");
                    }
                    socket1.close();
                }
                filePart = filePart + 1;
            }
        }


        else if(args[0].equals("retrieve")) {
            //RetrieveFileRequest to Controller
            //String mergedFile = absDir.toString() + "/clientDirectory/FileM.txt";
            String currPath = ".";
            Path p = Paths.get(currPath);
            Path absDir = p.toAbsolutePath();
            String fileRequired = args[1];
            String[] tokens = fileRequired.split("/");
            int length = tokens.length;
            String filename  = tokens[length-1].split(".")[0];
            String mergedFile = absDir.toString() + "/retrievedFilesDirectory/"+filename + ".txt";
            FileWriter writer = new FileWriter(mergedFile);
            RequestsToController.RetrieveFileRequest retrieveFileRequest = RequestsToController.RetrieveFileRequest.newBuilder()
                    .setFilename(filename)
                    .build();
            RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper1 = RequestsToController.RequestsToControllerWrapper.newBuilder().setRetrieveFileRequestMsg(retrieveFileRequest).build();
            System.out.println("Sending RetrieveFile request to Controller...");
            socket = new Socket(controllerHostname, controllerPort);
            requestsToControllerWrapper1.writeDelimitedTo(socket.getOutputStream());

            //Response from Controller with Storage Nodes list which host the replicas of chunks of given file
            System.out.println("Waiting for RetrieveFile response from Controller...");
            ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            System.out.println("Received RetrieveFile response from Controller...");
            socket.close();

            for (ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata : responseFromCN.getChunkListList()) {
                //chunkid needs to be get form controller node response but now as it is not fully implemented
                //needs to change it later
                RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = RequestsToStorageNode.RetrieveFileRequestToSN.newBuilder()
                        .setChunkId(chunkMetadata.getChunkId())
                        .setFilename(responseFromCN.getFilename())
                        .build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper toStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setRetrieveFileRequestToSNMsg(requestToSN).build();
                socket1 = new Socket(chunkMetadata.getNode().getHostname(),chunkMetadata.getNode().getPort());
                System.out.println("Sending RetrieveFile request to Storage Node...");
                toStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());
                System.out.println("Waiting for RetrieveFile response from Storage Node...");


                ResponsesToClient.RetrieveFileResponseFromSN responseFromSN = ResponsesToClient.RetrieveFileResponseFromSN.parseDelimitedFrom(socket1.getInputStream());
                System.out.println("Received RetrieveFile response from Storage Node...");
                byte[] temp = responseFromSN.getChunkData().toByteArray();
                int k = 0;
                while (k < temp.length) {
                    writer.write(temp[k]);
                    k++;
                }
                socket1.close();
            }

            writer.close();
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
        System.out.println(numBlocks);
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

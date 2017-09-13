package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileWriter;
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

        //chunking functionality
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String filePath = absDir.toString() + "/clientDirectory/File1.txt";
        String mergedFile = absDir.toString() + "/clientDirectory/FileM.txt";
        FileWriter writer = new FileWriter(mergedFile);

        byte[] bFile = Files.readAllBytes(new File(filePath).toPath());

        System.out.println(bFile.length);

        int i=0;
        int j=0;
        int blocksize = 2000;
        int filePart = 1;
        byte[] blockdata = new byte[blocksize];
        int filesize = bFile.length;
        int numBlocks  = (filesize / blocksize) ;
        if((filesize % blocksize) != 0)
            numBlocks = numBlocks + 1;
        System.out.println(numBlocks);
        List<byte[]> blocks = new ArrayList<>();

        while(i < filesize)
        {
            byte[] block = new byte[blocksize];
            for (j = 0; j < blocksize; j++)
            {
                if(i<filesize)
                {
                    block[j] = bFile[i];
                    i++;
                }
            }
            blocks.add(block);
        }

        for(byte[] block : blocks)
        {
            //sending block to controller with blockinfo
            //StoreFile request to Controller
            socket = new Socket("localhost",9998);
            String filename = "File1";
            RequestsToController.StoreChunkRequest storeChunk
                    = RequestsToController.StoreChunkRequest.newBuilder()
                    .setChunkId(filePart)
                    .setFilename(filename).build();
            RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                                                                                            .setStoreChunkRequestMsg(storeChunk).build();
            System.out.println("Sending StoreFile request to Controller...");
            requestsToControllerWrapper.writeDelimitedTo(socket.getOutputStream());

            System.out.println("Waiting for StoreFile response from Controller...");

            ResponsesToClient.StoreChunkResponse response =  ResponsesToClient.StoreChunkResponse.parseDelimitedFrom(socket.getInputStream());

            System.out.println("Received StoreFile response from Controller...");
            socket.close();

            //readinessCheck request to Storage Node
            socket1 = new Socket("localhost",response.getStorageNodeList(0).getPort());

            RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode readinessCheck = RequestsToStorageNode.ReadinessCheckRequestToSN.StorageNode.newBuilder()
                    .setPort(response.getStorageNodeList(0).getPort()).build();

            RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                                                                                        .addStorageNodeList(readinessCheck)
                                                                                        .build();

            RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                                .setReadinessCheckRequestToSNMsg(readinessCheckRequestToSN).build();


            System.out.println("Sending readinessCheck request to Storage Node...");
            requestsToStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());

            ResponsesToClient.AcknowledgeReadinessToClient acknowledgeReadinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.parseDelimitedFrom(socket1.getInputStream());

            System.out.println("Received readinessCheck response from Storage Node...");
            socket1.close();

            if(acknowledgeReadinessToClient.getSuccess())
            {
                //StoreFileRequest to Storage Node
                RequestsToStorageNode.StoreChunkRequestToSN.StorageNode storageNode = RequestsToStorageNode.StoreChunkRequestToSN.StorageNode.newBuilder()
                        .setPort(response.getStorageNodeList(0).getPort()).build();


                RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                        .addStorageNodeList(storageNode)
                        .setChunkId(filePart)
                        .setFilename(filename)
                        .setChunkData(ByteString.copyFrom(block)).build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                .setStoreChunkRequestToSNMsg(storeChunkRequestToSN).build();
                socket1 = new Socket("localhost",response.getStorageNodeList(0).getPort());
                System.out.println("Sending store chunk request to Storage Node...");
                wrapper.writeDelimitedTo(socket1.getOutputStream());
                System.out.println("Waiting for store chunk response from Storage Node...");

                ResponsesToClient.ResponsesToClientWrapper responsesToClientWrapper = ResponsesToClient.ResponsesToClientWrapper.parseDelimitedFrom(socket1.getInputStream());

                if(responsesToClientWrapper.hasAcknowledgeStoreChunkToClientMsg())
                {
                    if(responsesToClientWrapper.getAcknowledgeStoreChunkToClientMsg().getSuccess())
                        System.out.println("Received response from Storage Node!!success");
                    else
                        System.out.println("Received response from Storage Node!!fail");
                }
                socket1.close();
            }

            //RetrieveFileRequest to Controller
            RequestsToController.RetrieveFileRequest retrieveFileRequest = RequestsToController.RetrieveFileRequest.newBuilder()
                                                                            .setFilename(filename)
                                                                            .build();
            RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper1 = RequestsToController.RequestsToControllerWrapper.newBuilder().setRetrieveFileRequestMsg(retrieveFileRequest).build();
            System.out.println("Sending RetrieveFile request to Controller...");
            socket = new Socket("localhost",9998);
            requestsToControllerWrapper1.writeDelimitedTo(socket.getOutputStream());

            System.out.println("Waiting for RetrieveFile response from Controller...");
            ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            System.out.println("Received RetrieveFile response from Controller...");
            socket.close();

            for(ResponsesToClient.RetrieveFileResponseFromCN.storageNode storageNode : responseFromCN.getStorageNodeListList())
            {
                //chunkid needs to bet get form controller node response but now as it is not fully implemented
                //needs to change it later
                RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = RequestsToStorageNode.RetrieveFileRequestToSN.newBuilder()
                                                                            .setChunkId(filePart)
                                                                            .setFilename(filename)
                                                                            .build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper toStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                                            .setRetrieveFileRequestToSNMsg(requestToSN).build();
                socket1 = new Socket("localhost",storageNode.getPort());
                System.out.println("Sending RetrieveFile request to Storage Node...");
                toStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());
                System.out.println("Waiting for RetrieveFile response from Storage Node...");


                ResponsesToClient.RetrieveFileResponseFromSN responseFromSN = ResponsesToClient.RetrieveFileResponseFromSN.parseDelimitedFrom(socket1.getInputStream());
                System.out.println("Received RetrieveFile response from Storage Node...");
                byte[] temp = responseFromSN.getChunkData().toByteArray();
                int k=0;
                while(k < temp.length)
                {
                    writer.write(temp[k]);
                    k++;
                }
                socket1.close();
            }

            filePart = filePart + 1;
        }
        writer.close();


    }

}

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
            socket = new Socket("localhost",9998);
            String blockFile = absDir.toString() + "/data/" + "File1Part" + filePart  +".txt";
            StorageMessages.StoreChunk storeChunk
                    = StorageMessages.StoreChunk.newBuilder()
                    .setChunkId(filePart)
                    .setFileName(blockFile)
                    .setData(ByteString.copyFrom(block)).build();
            System.out.println("Sending request to Controller...");
            storeChunk.writeDelimitedTo(socket.getOutputStream());

            System.out.println("Waiting for response from Controller...");




            GetStorageNode.storageNodesList storageNodesList
                    = GetStorageNode.storageNodesList.parseDelimitedFrom(socket.getInputStream());

            System.out.println("Received response from Controller...");

            System.out.println(storageNodesList.getPortListList());

            socket1 = new Socket("localhost",storageNodesList.getPortList(0).getPort());

            storeChunk
                    = StorageMessages.StoreChunk.newBuilder()
                    .setChunkId(filePart)
                    .setFileName(blockFile)
                    //.addPortID(StorageMessages.StoreChunk.Port.newBuilder().setPortNum(storageNodesList.getPortList(0).getPort()))
                    .addPortID(StorageMessages.StoreChunk.Port.newBuilder().setPortNum(storageNodesList.getPortList(1).getPort()))
                    .setData(ByteString.copyFrom(block)).build();
            System.out.println("Sending request to Storage Node...");
            storeChunk.writeDelimitedTo(socket1.getOutputStream());

            System.out.println("Received response from Storage Node...");

            Status.StorageMessageWrapper wrapper = Status.StorageMessageWrapper.parseDelimitedFrom(socket1.getInputStream());

            if(wrapper.getRecvStatus().getSuccess())
            {
                System.out.println("stored successfully on datanode");
                byte[] temp = wrapper.getRetrvChunkdata().getData().toByteArray();
                int k=0;
                while(k < temp.length)
                {
                    writer.write(temp[k]);
                    k++;
                }
            }
            else
                System.out.println("Failed to store on datanode");


            socket.close();
            socket1.close();
            filePart = filePart + 1;
        }
        writer.close();


    }

}

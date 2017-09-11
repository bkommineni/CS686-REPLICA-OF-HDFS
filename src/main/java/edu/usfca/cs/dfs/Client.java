package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client {

    public static void main(String[] args) throws Exception{
        Socket socket = null;
        Socket socket1 = null;

        //chunking functionality
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String filePath = absDir.toString() + "/data/File1.txt";

        byte[] bFile = Files.readAllBytes(new File(filePath).toPath());

        System.out.println(bFile.length);

        int i=0;
        int j=1;
        int blocksize = 2000;
        int filePart = 1;
        byte[] blockdata = new byte[blocksize];
        while (i < bFile.length )
        {

            if(j==blocksize)
            {
                //sending block to controller with blockinfo
                socket = new Socket("localhost",9998);
                String blockFile = absDir.toString() + "/data/" + "File1Part" + filePart  +".txt";
                StorageMessages.StoreChunk storeChunk
                        = StorageMessages.StoreChunk.newBuilder()
                        .setChunkId(filePart)
                        .setFileName(blockFile)
                        .setData(ByteString.copyFrom(blockdata)).build();
                System.out.println("Sending request to Controller...");
                storeChunk.writeDelimitedTo(socket.getOutputStream());

                System.out.println("Waiting for response from Controller...");
                GetStorageNode.getStorageNode storageNode
                        = GetStorageNode.getStorageNode.parseDelimitedFrom(socket.getInputStream());

                System.out.println("Received response from Controller...");

                System.out.println(storageNode.getPort());

                socket1 = new Socket("localhost",storageNode.getPort());
                storeChunk
                        = StorageMessages.StoreChunk.newBuilder()
                        .setChunkId(filePart)
                        .setFileName(blockFile)
                        .setData(ByteString.copyFrom(blockdata)).build();
                System.out.println("Sending request to Storage Node...");
                storeChunk.writeDelimitedTo(socket1.getOutputStream());

                System.out.println("Received response from Storage Node...");

                Status.receivedStatus status = Status.receivedStatus.parseDelimitedFrom(socket1.getInputStream());

                if(status.getSuccess())
                    System.out.println("stored successfully on datanode");
                else
                    System.out.println("Failed to store on datanode");


                filePart = filePart + 1;
                blockdata = new byte[blocksize];

                j=1;
                socket.close();
                socket1.close();
            }

            i++;
            j++;
        }
    }

}

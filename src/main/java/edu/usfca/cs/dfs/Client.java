package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import java.net.Socket;

public class Client {

    public static void main(String[] args) throws Exception{
        Socket socket = new Socket("localhost",9998);
        ByteString bytes = ByteString.copyFromUtf8("hello world!!");
        StorageMessages.StoreChunk storeChunk
                = StorageMessages.StoreChunk.newBuilder()
                .setChunkId(1)
                .setFileName("File1.txt")
                .setData(bytes).build();
        System.out.println("Sending request to Controller...");
        storeChunk.writeDelimitedTo(socket.getOutputStream());
        System.out.println("Waiting for response from Controller...");
        GetStorageNode.getStorageNode storageNode
                = GetStorageNode.getStorageNode.parseDelimitedFrom(socket.getInputStream());

        System.out.println("Received response from Controller...");

        System.out.println(storageNode.getPort());

        socket.close();

        Socket socket1 = new Socket("localhost",storageNode.getPort());
        StorageMessages.StoreChunk storeChunk1
                = StorageMessages.StoreChunk.newBuilder()
                .setChunkId(1)
                .setFileName("File1.txt")
                .setData(bytes).build();
        System.out.println("Sending request to Storage Node...");
        storeChunk.writeDelimitedTo(socket1.getOutputStream());
        socket1.close();
    }

}

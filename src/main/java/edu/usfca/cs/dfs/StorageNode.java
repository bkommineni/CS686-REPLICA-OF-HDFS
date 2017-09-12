package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageNode {

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        new StorageNode().start();
    }

    private void start() throws Exception
    {
        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("Listening...");
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Request(socket)).start();
        }
    }

    public class Request implements Runnable {
        Socket connectionSocket = null;

        public Request(Socket connectionSocket) {
            this.connectionSocket = connectionSocket;
        }

        @Override
        public void run() {
            try {
                System.out.println("in thread:SN");
                StorageMessages.StoreChunk storeChunk
                        = StorageMessages.StoreChunk.parseDelimitedFrom(
                        connectionSocket.getInputStream());

                System.out.println("Storing file name: "
                        + storeChunk.getFileName());

                System.out.println(storeChunk.getData());
                System.out.println(storeChunk.getPortIDList());

                byte[] bytes = storeChunk.getData().toByteArray();

                int i=0;
                String currPath = ".";
                Path p = Paths.get(currPath);
                Path absDir = p.toAbsolutePath();
                String blockFile = absDir.toString() + "/data/" + "File1Part" + storeChunk.getChunkId() +"_SN" +".txt";
                FileWriter writer = new FileWriter(blockFile);

                while(i < bytes.length)
                {
                    writer.write(bytes[i]);
                    i++;
                }
                writer.close();

                Status.receivedStatus status = Status.receivedStatus.newBuilder().setSuccess(true).build();
                Status.retrieveChunkdata chunkdata = Status.retrieveChunkdata
                                                        .newBuilder()
                                                        .setFilename(blockFile)
                                                        .setData(ByteString.copyFrom(Files.readAllBytes(new File(blockFile).toPath())))
                                                        .build();
                Status.StorageMessageWrapper messageWrapper = Status.StorageMessageWrapper
                                                                .newBuilder()
                                                                .setRecvStatus(status)
                                                                .setRetrvChunkdata(chunkdata)
                                                                .build();
                messageWrapper.writeDelimitedTo(connectionSocket.getOutputStream());
            } catch (IOException e) {
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

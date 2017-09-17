package edu.usfca.cs.dfs;

import com.google.protobuf.Message;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Controller {

    /*
    Request Types
    1 Enroll request
    */

    private int controllerPort = 9998;
    private Map<String,DataNode> storageNodesList = new HashMap<>();
    private List<FileMetadata> fileMetadatas = new ArrayList<>();
    private Map<String,Metadata> metadataMap = new HashMap<>();

    public static void main(String[] args) throws Exception{
        new Controller().start(args);
    }

    private void start(String[] args) throws Exception
    {
        if(args[1] != null)
            controllerPort = Integer.parseInt(args[1]);
        String hostname = getHostname();
        System.out.println("Starting controller on " + hostname + " and port: "+ controllerPort + "...");
        ServerSocket serverSocket = new ServerSocket(9998);
        System.out.println("Listening...");
        while (true)
        {
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
                RequestsToController.RequestsToControllerWrapper msgWrapper = RequestsToController.RequestsToControllerWrapper
                                                                                .parseDelimitedFrom(connectionSocket.getInputStream());

                if(msgWrapper.hasEnrollMsg())
                {
                    //enroll storage node
                    storageNodesList.put(msgWrapper.getEnrollMsg().getHostname(),new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
                    acknowledgeEnrollment.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(msgWrapper.hasRetrieveFileRequestMsg())
                {
                    //retrieve file functionality
                    List<Metadata> metadatas = new ArrayList<>();
                    for(String str : metadataMap.keySet())
                    {
                        if(str.contains(msgWrapper.getRetrieveFileRequestMsg().getFilename()))
                        {
                            metadatas.add(metadataMap.get(str));
                        }
                    }

                    List<ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata> chunkMetadatas = new ArrayList<>();
                    for(Metadata metadata : metadatas)
                    {
                        ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.storageNode storageNode =
                                ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.storageNode.newBuilder()
                                .setPort(metadata.getDataNode().getPort())
                                .setHostname(metadata.getDataNode().getHostname())
                                .build();
                        ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata =
                                ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.newBuilder()
                                .setChunkId(metadata.getChunkId())
                                .setNode(storageNode)
                                .build();
                        chunkMetadatas.add(chunkMetadata);
                    }
                    ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN
                                                                                    .newBuilder()
                                                                                    .setFilename(msgWrapper.getRetrieveFileRequestMsg().getFilename())
                                                                                    .addAllChunkList(chunkMetadatas)
                                                                                    .build();
                    responseFromCN.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(msgWrapper.hasStoreChunkRequestMsg())
                {
                    //store file functionality
                    //allocate storage nodes for store file request
                    //when deploying on bass
                    /*Random rand = new Random();
                    List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();
                    for(int i=0;i<3;i++)
                    {
                        int portNum = rand.nextInt(24) + 1;
                        System.out.println("Port Number : "+portNum);
                        StringBuffer buffer = new StringBuffer();
                        if(portNum < 10)
                            buffer.append("bass0");
                        else
                            buffer.append("bass");
                        DataNode storageNode = storageNodesList.get(buffer.toString()+Integer.toString(portNum)+".cs.usfca.edu");
                        ResponsesToClient.StoreChunkResponse.storageNode storageNodeMsg =
                                ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(storageNode.getPort())
                                                                                             .setHostname(storageNode.getHostname())
                                                                                             .build();
                        storageNodes.add(storageNodeMsg);
                    }*/
                    List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();
                    ResponsesToClient.StoreChunkResponse.storageNode storageNodeMsg =
                            ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(9999)
                                    .setHostname("localhost")
                                    .build();
                    storageNodes.add(storageNodeMsg);

                    //return the set of nodes/numbers randomly generated in response
                    ResponsesToClient.StoreChunkResponse.Builder builder = ResponsesToClient.StoreChunkResponse.newBuilder();
                    ResponsesToClient.StoreChunkResponse storeChunkResponse = builder.addAllStorageNodeList(storageNodes).build();
                    storeChunkResponse.writeDelimitedTo(connectionSocket.getOutputStream());
                }

                if(msgWrapper.hasAcknowledgeStoreChunkMsg())
                {
                    //acknowledge store chunk
                    //updating blocks and nodes info based on acknowledgement
                    RequestsToController.AcknowledgeStoreChunk acknowledge = msgWrapper.getAcknowledgeStoreChunkMsg();

                    /*ChunkMetadata chunk = new ChunkMetadata(acknowledge.getFilename(),acknowledge.getChunkId());
                    ReplicaMetadata replica = new ReplicaMetadata(acknowledge.getFilename(),acknowledge.getChunkId(),
                            new DataNode(acknowledge.getPort(),acknowledge.getHostname()));
                    chunk.addReplicaMetadata(replica);
                    FileMetadata fileMetadata = new FileMetadata(acknowledge.getFilename());
                    fileMetadata.addChunkMetadata(chunk);
                    fileMetadatas.add(fileMetadata);*/

                    String key = acknowledge.getFilename() + acknowledge.getChunkId() + acknowledge.getHostname();
                    if(!metadataMap.containsKey(key))
                    {
                        Metadata metadata = new Metadata(acknowledge.getFilename(),acknowledge.getChunkId());
                        metadata.setDataNode(new DataNode(acknowledge.getPort(),acknowledge.getHostname()));
                        metadataMap.put(key,metadata);
                    }
                }

                if(msgWrapper.hasHeartbeatMsg())
                {
                    //check info sent on heartbeat and make sure what are active nodes


                }

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

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
    private int counter = 0;
    private int noOfStorageNodesDeployed = 0;
    private boolean statusStorageNodes[] = null;

    public static void main(String[] args) throws Exception{
        new Controller().start(args);
    }

    /**/
    private void start(String[] args) throws Exception
    {
        if(args.length > 0 ) {
            if (args[0] != null)
                controllerPort = Integer.parseInt(args[0]);
            if(args[1] != null)
                noOfStorageNodesDeployed = Integer.parseInt(args[1]);
        }
        statusStorageNodes = new boolean[noOfStorageNodesDeployed];
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
                    statusStorageNodes[msgWrapper.getEnrollMsg().getPort()] = true;
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
                    System.out.println("entering store chunk in controller!!");
                    Random rand = new Random();
                    List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();

                    int count = 1;
                    while(count <= 3)
                    {
                        int nodeNum = rand.nextInt(noOfStorageNodesDeployed) + 1;
                        if(statusStorageNodes[nodeNum])
                        {
                            System.out.println("Node Number : " + nodeNum);
                            StringBuilder builder = new StringBuilder();
                            if (nodeNum < 10)
                                builder.append("bass0");
                            else
                                builder.append("bass");
                            DataNode storageNode = storageNodesList.get(builder.toString() + Integer.toString(nodeNum) + ".cs.usfca.edu");
                            ResponsesToClient.StoreChunkResponse.storageNode storageNodeMsg =
                                    ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(storageNode.getPort())
                                            .setHostname(storageNode.getHostname())
                                            .build();
                            storageNodes.add(storageNodeMsg);
                            count++;
                        }
                    }

                    //return the set of nodes/numbers randomly generated in response
                    ResponsesToClient.StoreChunkResponse.Builder builder = ResponsesToClient.StoreChunkResponse.newBuilder();
                    ResponsesToClient.StoreChunkResponse storeChunkResponse = builder.addAllStorageNodeList(storageNodes).build();
                    storeChunkResponse.writeDelimitedTo(connectionSocket.getOutputStream());
                    System.out.println("coming out of store chunk in controller");
                }

                if(msgWrapper.hasHeartbeatMsg())
                {
                    //check info sent on heartbeat and make sure what are active nodes
                    int size = msgWrapper.getHeartbeatMsg().getMetadataList().size();
                    RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                                                                                .setHostname(msgWrapper.getHeartbeatMsg().getSN().getHostname())
                                                                                .setPort(msgWrapper.getHeartbeatMsg().getSN().getPort())
                                                                                .build();
                    for(int i=0;i<size;i++)
                    {
                        RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = msgWrapper.getHeartbeatMsg().getMetadataList().get(i);
                        String key = chunkMetadata.getFilename() + chunkMetadata.getChunkId() + storageNode.getHostname();
                        if(!metadataMap.containsKey(key))
                        {
                            Metadata metadata = new Metadata(chunkMetadata.getFilename(),chunkMetadata.getChunkId());
                            metadata.setDataNode(new DataNode(storageNode.getPort(),storageNode.getHostname()));
                            metadataMap.put(key,metadata);
                        }
                    }

                }
                if(msgWrapper.hasListOfActiveNodes())
                {
                    List<ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNode> storageNodes = new ArrayList<>();
                    for(int i=0;i<statusStorageNodes.length;i++)
                    {
                        if(statusStorageNodes[i])
                        {
                            StringBuilder builder = new StringBuilder();
                            if (i < 10)
                                builder.append("bass0");
                            else
                                builder.append("bass");
                            DataNode storageNode = storageNodesList.get(builder.toString() + Integer.toString(i) + ".cs.usfca.edu");
                            ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNode storageNodeMsg =
                                    ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNode.newBuilder().setPort(storageNode.getPort())
                                            .setHostname(storageNode.getHostname())
                                            .build();
                            storageNodes.add(storageNodeMsg);
                        }
                    }
                    ResponsesToClient.ListOfActiveStorageNodesResponseFromCN list = ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.newBuilder()
                                                                                    .addAllActiveStorageNodes(storageNodes).build();
                    list.writeDelimitedTo(connectionSocket.getOutputStream());
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

package edu.usfca.cs.dfs;

import com.google.protobuf.Message;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

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
    private int noOfStorageNodesDeployed = 24;
    private boolean statusStorageNodes[] = null;
    private enum StorageNodes
    {
        bass01,bass02,bass03,bass04,bass05,bass06,bass07,bass08,bass09,
        bass10,bass11,bass12,bass13,bass14,bass15,bass16,bass17,bass18,
        bass19,bass20,bass21,bass22,bass23,bass24;

        int getIndexForStorageNode()
        {
            switch (this)
            {
                case bass01:
                    return 1;
                case bass02:
                    return 2;
                case bass03:
                    return 3;
                case bass04:
                    return 4;
                case bass05:
                    return 5;
                case bass06:
                    return 6;
                case bass07:
                    return 7;
                case bass08:
                    return 8;
                case bass09:
                    return 9;
                case bass10:
                    return 10;
                case bass11:
                    return 11;
                case bass12:
                    return 12;
                case bass13:
                    return 13;
                case bass14:
                    return 14;
                case bass15:
                    return 15;
                case bass16:
                    return 16;
                case bass17:
                    return 17;
                case bass18:
                    return 18;
                case bass19:
                    return 19;
                case bass20:
                    return 20;
                case bass21:
                    return 21;
                case bass22:
                    return 22;
                case bass23:
                    return 23;
                case bass24:
                    return 24;
                default:
                    throw new AssertionError("Unknown operations " + this);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        new Controller().start(args);
    }

    /**/
    private void start(String[] args) throws Exception
    {
        if(args.length > 0 ) {
            if (args[0] != null)
                controllerPort = Integer.parseInt(args[0]);
            /*if(args[1] != null)
                noOfStorageNodesDeployed = Integer.parseInt(args[1]);*/
        }
        statusStorageNodes = new boolean[noOfStorageNodesDeployed];
        String hostname = getHostname();
        System.out.println("Starting controller on " + hostname + " and port: "+ controllerPort + "...");
        ServerSocket serverSocket = new ServerSocket(controllerPort);
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
                    System.out.println("Received enrollment request from storage node");
                    String hostname = msgWrapper.getEnrollMsg().getHostname();
                    String[] tokens = hostname.split("\\.");
                    storageNodesList.put(hostname,new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    statusStorageNodes[StorageNodes.valueOf(tokens[0]).getIndexForStorageNode()] = true;
                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
                    acknowledgeEnrollment.writeDelimitedTo(connectionSocket.getOutputStream());
                    System.out.println("Enrollment done!And acknowedged storage node with response");
                }

                if(msgWrapper.hasRetrieveFileRequestMsg())
                {
                    //retrieve file functionality
                    System.out.println("Received retrieve file request from client");
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
                    System.out.println("Responded with list of three distict storage nodes to client for retrieve file request");
                }

                if(msgWrapper.hasStoreChunkRequestMsg())
                {
                    //store file functionality
                    //allocate storage nodes for store file request
                    //when deploying on bass
                    System.out.println("Received store chunk request from client");
                    List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();

                    int count = 1;
                    List<Integer> nodenums = new ArrayList<>();
                    while(count <= 3)
                    {
                        int nodeNum = ThreadLocalRandom.current().nextInt(1, 24);
                        if(statusStorageNodes[nodeNum] && (!nodenums.contains(nodeNum)))
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
                    System.out.println("Responded with list of three distict storage nodes to client for store chunk request");
                }

                if(msgWrapper.hasHeartbeatMsg())
                {
                    //check info sent on heartbeat and make sure what are active nodes
                    System.out.println("Received heartbeat message from storage node " + msgWrapper.getHeartbeatMsg().getSN().getHostname());
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
                    System.out.println("Updated info from heartbeat message in memory");

                }
                if(msgWrapper.hasListOfActiveNodes())
                {
                    System.out.println("Received request for list of active storage nodes");
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
                    System.out.println("Responded with a list of active storage nodes");
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

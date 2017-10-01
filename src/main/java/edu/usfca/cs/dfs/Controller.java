package edu.usfca.cs.dfs;

import com.google.protobuf.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Controller {

    /*
    Request Types
    1 Enroll request
    */

    private int controllerPort;
    private Map<String,DataNode> storageNodesList = new HashMap<>();
    private Map<String,Metadata> metadataMap = new HashMap<>();
    private Map<String,Boolean>  statusStorageNodesMap = new HashMap<>();
    private Map<Integer,String>  storageNodeMapToNum  = new HashMap<>();
    int counter = 1;

    public static void main(String[] args) throws Exception{
        new Controller().start(args);
    }

    /**/
    private void start(String[] args) throws Exception
    {
        if(args.length > 0 ) {
            if (args[0] != null)
                controllerPort = Integer.parseInt(args[0]);
        }
        /*Setting up the nodes using nodes list from config file*/

        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String configPath = absDir.toString() + "/config/Storage-nodes-list.txt";
        BufferedReader reader = new BufferedReader(new FileReader(configPath));
        String str = null;
        while ((str = reader.readLine()) != null)
        {
            statusStorageNodesMap.put(str,false);
            storageNodeMapToNum.put(counter,str);
	    counter = (counter + 1);
        }

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
                    storageNodesList.put(hostname,new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    //setting storage node to active
                    statusStorageNodesMap.put(hostname,true);
                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
		    System.out.println("enrolled host : "+hostname);
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
		    for(int num : storageNodeMapToNum.keySet())
                        {
                                System.out.println(storageNodeMapToNum.get(num));
                        }	
                    while(count <= 3)
                    {
			Random r = new Random();
			int nodeNum = r.nextInt(storageNodeMapToNum.size())+1;
                        //int nodeNum = ThreadLocalRandom.current().nextInt(1, storageNodeMapToNum.size());
			//System.out.println(nodeNum + " "+"while loop");
                        if(storageNodeMapToNum.get(nodeNum) != null)
                        {
			    //System.out.println("if loop...."+nodeNum+"--"+statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum))+"--"+nodenums.contains(nodeNum));
                            if (statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)) && (!nodenums.contains(nodeNum))) {
                                System.out.println("Node Number : " + nodeNum);
                                DataNode storageNode = storageNodesList.get(storageNodeMapToNum.get(nodeNum));
				System.out.println("Node hostname : " + storageNodeMapToNum.get(nodeNum));
                                ResponsesToClient.StoreChunkResponse.storageNode storageNodeMsg =
                                        ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(storageNode.getPort())
                                                .setHostname(storageNode.getHostname())
                                                .build();
                                storageNodes.add(storageNodeMsg);
                                nodenums.add(nodeNum);
                                count++;
                            }
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
                    for(String str : statusStorageNodesMap.keySet())
                    {
                        if(statusStorageNodesMap.get(str))
                        {
                            DataNode storageNode = storageNodesList.get(str);
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

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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class Controller {

    /*
    Request Types
    1 Enroll request
    */

    public static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private int controllerPort;
    private Map<String,DataNode> storageNodesList = new HashMap<>();
    private Map<String,Metadata> metadataMap = new HashMap<>();
    private Map<String,Boolean>  statusStorageNodesMap = new HashMap<>();
    private Map<Integer,String>  storageNodeMapToNum  = new HashMap<>();

    private static final int REPLICATION_FACTOR = 3;

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
        int counter = 1;
        while ((str = reader.readLine()) != null)
        {
            statusStorageNodesMap.put(str,false);
            storageNodeMapToNum.put(counter,str);
	    counter = (counter + 1);
        }

        String hostname = getHostname();
        logger.info("Starting controller on " + hostname + " and port: "+ controllerPort + "...");
        ServerSocket serverSocket = new ServerSocket(controllerPort);
        logger.info("Listening...");
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
                InetAddress inetAddress = connectionSocket.getInetAddress();
                int port = connectionSocket.getPort();

                if(msgWrapper.hasEnrollMsg())
                {
                    //enroll storage node

                    String hostname = msgWrapper.getEnrollMsg().getHostname();
                    logger.info("Received enrollment request from storage node {} from port {}",hostname,port);
                    //setting storage node to active
                    if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                    {
                        statusStorageNodesMap.put(hostname+msgWrapper.getEnrollMsg().getPort(),true);
                        storageNodesList.put(hostname+msgWrapper.getEnrollMsg().getPort(),
                                new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    }
                    else
                    {
                        statusStorageNodesMap.put(hostname,true);
                        storageNodesList.put(hostname,
                                new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    }

                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
		            logger.info("enrolled host : {}",hostname);
                    acknowledgeEnrollment.writeDelimitedTo(connectionSocket.getOutputStream());
                    logger.info("Enrollment done!And acknowedged storage node with response");
                    connectionSocket.close();
                }

                if(msgWrapper.hasRetrieveFileRequestMsg())
                {
                    //retrieve file functionality
                    logger.info("Received retrieve file request from client {} from port {}",inetAddress,port);
                    List<Metadata> metadatas = new ArrayList<>();
                    String filename = msgWrapper.getRetrieveFileRequestMsg().getFilename();
                    for(String str : metadataMap.keySet())
                    {
                        if(filename.contains(".txt"))
                        {
                            String tokens[] = filename.split("\\.");
                            if(str.contains(tokens[0]))
                            {
                                Metadata metadata = metadataMap.get(str);
                                if(statusStorageNodesMap.get(metadata.getDataNode().getHostname()+metadata.getDataNode().getPort()))
                                {
                                    if(!metadatas.contains(metadata))
                                    {
                                        metadatas.add(metadataMap.get(str));
                                    }
                                }
                            }
                        }
                        else
                        {
                            if(str.contains(filename))
                            {
                                Metadata metadata = metadataMap.get(str);
                                if(statusStorageNodesMap.get(metadata.getDataNode().getHostname()+metadata.getDataNode().getPort()))
                                {
                                    if(!metadatas.contains(metadata))
                                    {
                                        metadatas.add(metadataMap.get(str));
                                    }
                                }
                            }
                        }

                    }
                    logger.info("metadatas {}",metadatas);

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
                                .setFilename(msgWrapper.getRetrieveFileRequestMsg().getFilename())
                                .setNode(storageNode)
                                .build();
                        chunkMetadatas.add(chunkMetadata);
                    }
                    ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN
                                                                                    .newBuilder()
                                                                                    .addAllChunkList(chunkMetadatas)
                                                                                    .build();
                    responseFromCN.writeDelimitedTo(connectionSocket.getOutputStream());
                    logger.info("Responded with list of three distinct storage nodes to client for retrieve file request");
                    connectionSocket.close();
                }

                if(msgWrapper.hasStoreChunkRequestMsg())
                {
                    //store file functionality
                    //allocate storage nodes for store file request
                    //when deploying on bass
                    logger.info("Received store chunk request from client {} from port {}",inetAddress,port);
                    List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();

                    int count = 1;
                    List<Integer> nodenums = new ArrayList<>();
		            for(int num : storageNodeMapToNum.keySet())
                    {
                            logger.debug(storageNodeMapToNum.get(num));
                    }
                    while(count <= REPLICATION_FACTOR)
                    {
			            Random r = new Random();
			            int nodeNum = r.nextInt(storageNodeMapToNum.size())+1;
			            logger.debug(nodeNum + " "+"while loop");
                        if(storageNodeMapToNum.get(nodeNum) != null)
                        {
			                logger.debug("if loop...."+nodeNum+"--"+statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum))+"--"+nodenums.contains(nodeNum));
                            if (statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)) && (!nodenums.contains(nodeNum))) {
                                logger.info("Replica Node Number {} Replica Node hostname {} " ,nodeNum,storageNodeMapToNum.get(nodeNum));
                                DataNode storageNode = storageNodesList.get(storageNodeMapToNum.get(nodeNum));
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
                    logger.info("Responded with list of three distinct storage nodes to client for store chunk request");
                    connectionSocket.close();
                }

                if(msgWrapper.hasHeartbeatMsg())
                {
                    //check info sent on heartbeat and make sure what are active nodes
                    logger.info("Received heartbeat message from storage node {} from port {}" , msgWrapper.getHeartbeatMsg().getSN().getHostname(),msgWrapper.getHeartbeatMsg().getSN().getPort());
                    int size = msgWrapper.getHeartbeatMsg().getMetadataList().size();
                    RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                                                                                .setHostname(msgWrapper.getHeartbeatMsg().getSN().getHostname())
                                                                                .setPort(msgWrapper.getHeartbeatMsg().getSN().getPort())
                                                                                .build();
                    for(int i=0;i<size;i++)
                    {
                        RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = msgWrapper.getHeartbeatMsg().getMetadataList().get(i);
                        String key = chunkMetadata.getFilename() + chunkMetadata.getChunkId() + storageNode.getHostname();
                        logger.info("metadata map key {}",key);
                        if(!metadataMap.containsKey(key))
                        {
                            Metadata metadata = new Metadata(chunkMetadata.getFilename(),chunkMetadata.getChunkId());
                            metadata.setDataNode(new DataNode(storageNode.getPort(),storageNode.getHostname()));
                            metadataMap.put(key,metadata);
                        }
                    }
                    logger.debug("Updated info from heartbeat message in memory from SN {} from port {}",msgWrapper.getHeartbeatMsg().getSN().getHostname(),msgWrapper.getHeartbeatMsg().getSN().getPort());
                    connectionSocket.close();

                }
                if(msgWrapper.hasListOfActiveNodes())
                {
                    logger.info("Received request for list of active storage nodes from client {} from port {}",inetAddress,port);
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
                    logger.info("Responded with a list of active storage nodes");
                    connectionSocket.close();
                }

            } catch (IOException e) {
                logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
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

package edu.usfca.cs.dfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class Controller {

    /*
    Request Types
    1 Enroll request
    */

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private int controllerPort;
    private Map<String,DataNode> storageNodesList = new HashMap<>();
    private Map<String,Metadata> metadataMap = new HashMap<>();
    private Map<String,Boolean>  statusStorageNodesMap = new HashMap<>();
    private Map<Integer,String>  storageNodeMapToNum  = new HashMap<>();
    private Map<String,Long>     storageNodeHeartBeatTimeStamps = new HashMap<>();
    public static final int NUM_THREADS_ALLOWED = 15;
    private ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);
    public static final Long MAX_ALLOWED_ACTIVENESS = 10000L;
    private String configPath = null;

    private static final int REPLICATION_FACTOR = 3;

    public static void main(String[] args) throws Exception{
        new Controller().start(args);
    }

    /**/
    private void start(String[] args) throws Exception
    {
        if(args.length > 0 ) {
            if (args[0] != null) {
                controllerPort = Integer.parseInt(args[0]);
                if(args[1] != null)
                {
                    configPath = args[1];
                }
            }
        }
        /*Setting up the nodes using nodes list from config file*/
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
            executorService.submit(new Thread(new Request(socket)));
        }
    }

    public class ActivenessChecker implements Runnable
    {
        String hostname = null;
        int port ;
        public ActivenessChecker(String hostname,int port)
        {
            this.hostname = hostname;
            this.port = port;
        }

        @Override
        public void run()
        {
            String key = hostname+Integer.toString(port);
            logger.info("Activeness check started on hostname {} port {}",hostname,port);
            while(true)
            {
                if (storageNodeHeartBeatTimeStamps.get(key) != null)
                {
                    Long prevTimeStamp = storageNodeHeartBeatTimeStamps.get(key);
                    Long currentTimeStamp = System.currentTimeMillis();
                    Long diff = (currentTimeStamp - prevTimeStamp);
                    logger.debug("diff {} prevTimeStamp {} currTImeStamp {}",diff,prevTimeStamp,currentTimeStamp);
                    if (diff > MAX_ALLOWED_ACTIVENESS)
                    {
			            logger.info("setting status as false for host {} port {}",hostname,port);
                        //remove all metadata related particular storage node
                        String key1 = hostname+Integer.toString(port);
                        statusStorageNodesMap.put(key1,false);
                        storageNodesList.remove(key1);
                        Iterator<Map.Entry<String,Metadata>> iterator = metadataMap.entrySet().iterator();
                        List<Metadata> filenameChunkIdInfo = new ArrayList<>();
                        while (iterator.hasNext())
                        {
                            Map.Entry<String,Metadata> entry = iterator.next();
                            if(entry.getKey().contains(key1))
                            {
                                filenameChunkIdInfo.add(entry.getValue());
                                iterator.remove();
                            }
                        }
                        logger.info("deactivated {} {}",hostname,port);

                        //Replica which are there in this node needs to be stored in some other storage node
                        logger.info("Metadata of deactivated node");
                        for(Metadata metadata : filenameChunkIdInfo)
                        {
                            logger.info("filename {} chunkid {}",metadata.getFilename(),metadata.getChunkId());
                        }

                        List<Metadata>replicaNodeMetadatas = new ArrayList<>();
                        List<DataNode> dataNodeList = new ArrayList<>();
                        List<String> checkDuplicates = new ArrayList<>();

                        for(Metadata metadata : filenameChunkIdInfo)
                        {
                            iterator = metadataMap.entrySet().iterator();
                            while (iterator.hasNext())
                            {
                                Map.Entry<String, Metadata> entry = iterator.next();
                                if (entry.getKey().contains(metadata.getFilename() + metadata.getChunkId()))
                                {
                                    if(!checkDuplicates.contains(metadata.getFilename() + metadata.getChunkId()))
                                    {
                                        Metadata metadata1 = new Metadata(metadata.getFilename(), metadata.getChunkId());
                                        checkDuplicates.add(metadata.getFilename() + metadata.getChunkId());
                                        metadata1.setDataNode(new DataNode(entry.getValue().getDataNode().getPort(), entry.getValue().getDataNode().getHostname()));
                                        replicaNodeMetadatas.add(metadata1);
                                    }
                                    dataNodeList.add(new DataNode(entry.getValue().getDataNode().getPort(), entry.getValue().getDataNode().getHostname()));
                                }
                            }
                        }
                        logger.info("nodes which have same replica!!!");
                        for (Metadata metadata : replicaNodeMetadatas)
                        {
                            logger.info("hostname {} port {}", metadata.getDataNode().getHostname(), metadata.getDataNode().getPort());
                        }
                        logger.info("datanodes which has the same replica!!");
                        for(DataNode dataNode : dataNodeList)
                        {
                            logger.info("datanode {}",dataNode.toString());
                        }

                        int count = 1;
                        DataNode replicaCopyToBeSentTo = null;
                        while(count <= 1)
                        {
                            Random r = new Random();
                            int nodeNum = r.nextInt(storageNodeMapToNum.size())+1;
                            logger.info("node hostname {}",storageNodeMapToNum.get(nodeNum));
                            for(int i : storageNodeMapToNum.keySet())
                            {
                                logger.debug("keys in MapToNum {}",storageNodeMapToNum.get(i));
                            }
                            if(storageNodeMapToNum.get(nodeNum) != null)
                            {
                                String hostnamePort = storageNodeMapToNum.get(nodeNum);
                                boolean statusOfNode = statusStorageNodesMap.get(hostnamePort);
                                DataNode dataNode = storageNodesList.get(hostnamePort);

                                for(String str : storageNodesList.keySet())
                                {
                                    logger.debug("storage nodes list elements {}",storageNodesList.get(str).toString());
                                }
                                if(statusOfNode)
                                {
                                    if(!dataNodeList.contains(dataNode))
                                    {
                                        logger.info("Found 1 {}",dataNode.toString());
                                        replicaCopyToBeSentTo = storageNodesList.get(storageNodeMapToNum.get(nodeNum));
                                        count++;
                                        break;
                                    }
                                }
                            }
                        }
                        logger.info("replica to be sent to datanode {}",replicaCopyToBeSentTo.toString());
                        for(Metadata metadata : replicaNodeMetadatas)
                        {
                            try
                            {
                                logger.info("Send Replica Copy to SN {} to port {}",replicaCopyToBeSentTo.getHostname(), replicaCopyToBeSentTo.getPort());
                                Socket socket = new Socket(metadata.getDataNode().getHostname(),metadata.getDataNode().getPort());
                                RequestsToStorageNode.SendReplicaCopyToSN.storageNode storageNode = RequestsToStorageNode.SendReplicaCopyToSN.storageNode.newBuilder()
                                        .setHostname(replicaCopyToBeSentTo.getHostname())
                                        .setPort(replicaCopyToBeSentTo.getPort()).build();
                                RequestsToStorageNode.SendReplicaCopyToSN replicaCopyToSN = RequestsToStorageNode.SendReplicaCopyToSN.newBuilder()
                                        .setSN(storageNode)
                                        .setFilename(metadata.getFilename())
                                        .setChunkId(metadata.getChunkId()).build();
                                RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                        .newBuilder()
                                        .setSendReplicaCopyToSNMsg(replicaCopyToSN).build();
                                wrapper.writeDelimitedTo(socket.getOutputStream());
                                socket.close();
                            }
                            catch (IOException e)
                            {
                                logger.error("Exception Caught {}",ExceptionUtils.getStackTrace(e));
                            }
                        }
                        break;
                    }
                }
            }
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
			            logger.debug("enrolling and setting status to true..");
                        statusStorageNodesMap.put(hostname,true);
			            logger.debug("status after enrolling {} of host {}",statusStorageNodesMap.get(hostname),hostname);
                        storageNodesList.put(hostname,
                                new DataNode(msgWrapper.getEnrollMsg().getPort(),msgWrapper.getEnrollMsg().getHostname()));
                    }

                    ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                                                                                            .newBuilder().setSuccess(true).build();
		            logger.info("enrolled host : {}",hostname);
                    acknowledgeEnrollment.writeDelimitedTo(connectionSocket.getOutputStream());
                    logger.info("Enrollment done!And acknowedged storage node with response");
                    executorService.submit(new Thread(new ActivenessChecker(hostname,msgWrapper.getEnrollMsg().getPort())));
                    connectionSocket.close();
                }

                if(msgWrapper.hasRetrieveFileRequestMsg())
                {
                    //retrieve file functionality
                    logger.info("Received retrieve file request from client {} from port {}",inetAddress,port);
                    List<Metadata> metadatas = new ArrayList<>();
                    String filename = msgWrapper.getRetrieveFileRequestMsg().getFilename();
		            logger.debug("retrieve file request filename {}",filename);
                    for(String str : metadataMap.keySet())
                    {
                        logger.debug("metadata map key {}",str);
                        if(str.contains(filename))
                        {
                            Metadata metadata = metadataMap.get(str);
                            String hostname = metadata.getDataNode().getHostname();
                            logger.debug("hostname {} port {}",metadata.getDataNode().getHostname(),metadata.getDataNode().getPort());
                            if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                            {
                                if(statusStorageNodesMap.get(metadata.getDataNode().getHostname()+metadata.getDataNode().getPort()))
                                {
                                    logger.debug("status checked!!");
                                    if(!metadatas.contains(metadata))
                                    {
                                        metadatas.add(metadataMap.get(str));
                                    }
                                }
                            }
                            else
                            {
                                if(statusStorageNodesMap.get(metadata.getDataNode().getHostname()))
                                {
                                    logger.debug("status checked!!");
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
			            logger.debug("nodenum {}",nodeNum);
                        if(storageNodeMapToNum.get(nodeNum) != null)
                        {
			                logger.debug("if loop...."+nodeNum+"--"+statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum))+"--"+nodenums.contains(nodeNum));
			                logger.debug("status of storageNode {} list check {} on host {}",statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)),nodenums.contains(nodeNum),storageNodeMapToNum.get(nodeNum));
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
                    //logger.debug("Received heartbeat message from storage node {} from port {}" , msgWrapper.getHeartbeatMsg().getSN().getHostname(),msgWrapper.getHeartbeatMsg().getSN().getPort());
                    int size = msgWrapper.getHeartbeatMsg().getMetadataList().size();
                    RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                                                                                .setHostname(msgWrapper.getHeartbeatMsg().getSN().getHostname())
                                                                                .setPort(msgWrapper.getHeartbeatMsg().getSN().getPort())
                                                                                .build();
                    for(int i=0;i<size;i++)
                    {
                        RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = msgWrapper.getHeartbeatMsg().getMetadataList().get(i);
                        String key = chunkMetadata.getFilename() + chunkMetadata.getChunkId() + storageNode.getHostname() + storageNode.getPort();
                        //logger.debug("metadata map key {}",key);
                        if(!metadataMap.containsKey(key))
                        {
                            Metadata metadata = new Metadata(chunkMetadata.getFilename(),chunkMetadata.getChunkId());
                            metadata.setDataNode(new DataNode(storageNode.getPort(),storageNode.getHostname()));
                            metadataMap.put(key,metadata);
                        }
                    }
                    storageNodeHeartBeatTimeStamps.put(storageNode.getHostname()+storageNode.getPort(),System.currentTimeMillis());
                    //logger.debug("Updated info from heartbeat message in memory from SN {} from port {}",msgWrapper.getHeartbeatMsg().getSN().getHostname(),msgWrapper.getHeartbeatMsg().getSN().getPort());
                    connectionSocket.close();
                }
                if(msgWrapper.hasListOfActiveNodes())
                {
                    logger.info("Received request for list of active storage nodes from client {} from port {}",inetAddress,port);
                    List<ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo> storageNodes = new ArrayList<>();
                    for(String str : statusStorageNodesMap.keySet())
                    {
                        if(statusStorageNodesMap.get(str))
                        {
                            List<String> filenames = new ArrayList<>();
                            for(String key : metadataMap.keySet())
                            {
                                if(key.contains(str))
                                {
                                    String filename = metadataMap.get(key).getFilename();
                                    if(!filenames.contains(filename))
                                    {
                                        filenames.add(filename);
                                        DataNode storageNode = metadataMap.get(key).getDataNode();
                                        ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo storageNodeMsg =
                                                ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo.newBuilder().setPort(storageNode.getPort())
                                                        .setHostname(storageNode.getHostname())
                                                        .setFilename(metadataMap.get(key).getFilename())
                                                        .build();
                                        storageNodes.add(storageNodeMsg);
                                    }
                                }
                            }

                        }
                    }
                    ResponsesToClient.ListOfActiveStorageNodesResponseFromCN list = ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.newBuilder()
                                                                                    .addAllActiveStorageNodes(storageNodes).build();
                    list.writeDelimitedTo(connectionSocket.getOutputStream());
                    logger.info("Responded with a list of active storage nodes");
                    connectionSocket.close();
                }
                if(msgWrapper.hasSendGoodChunkRequestMsg())
                {
                    RequestsToController.SendGoodChunkRequest goodChunkRequest = msgWrapper.getSendGoodChunkRequestMsg();
                    ResponsesToStorageNode.GoodChunkInfoToSN goodChunkInfoToSN = null;
                    ResponsesToStorageNode.GoodChunkInfoToSN.storageNode SN = null;
                    String key = goodChunkRequest.getFilename()+goodChunkRequest.getChunkId();
                    for(Map.Entry<String,Metadata> entry : metadataMap.entrySet())
                    {
			logger.info("map entry {} protobuf msg {}",entry.getValue().getDataNode().getHostname(),goodChunkRequest.getSN().getHostname());
			logger.info("map entry port {} protobuf msg port {}",entry.getValue().getDataNode().getPort(),goodChunkRequest.getSN().getPort());
                        if(entry.getKey().contains(key))
                        {
                            if(goodChunkRequest.getSN().getHostname().equals("Bhargavis-MacBook-Pro.local"))
                            {
                                if(entry.getValue().getDataNode().getPort() != goodChunkRequest.getSN().getPort())
                                {
                                    SN = ResponsesToStorageNode.GoodChunkInfoToSN.storageNode.newBuilder()
                                            .setHostname(entry.getValue().getDataNode().getHostname())
                                            .setPort(entry.getValue().getDataNode().getPort()).build();
                                    goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN.newBuilder()
                                            .setFilename(entry.getValue().getFilename())
                                            .setChunkId(entry.getValue().getChunkId())
                                            .setSN(SN).build();
				    logger.info("Sending good chunk info to SN hostname {} port {}",entry.getValue().getDataNode().getHostname(),
                                                entry.getValue().getDataNode().getPort());
                               	    goodChunkInfoToSN.writeDelimitedTo(connectionSocket.getOutputStream());
                                    connectionSocket.close();
                                    break;
                                }
                            }
                            else if(!entry.getValue().getDataNode().getHostname().equals(goodChunkRequest.getSN().getHostname()))
                            {
                                SN = ResponsesToStorageNode.GoodChunkInfoToSN.storageNode.newBuilder()
                                        .setHostname(entry.getValue().getDataNode().getHostname())
                                        .setPort(entry.getValue().getDataNode().getPort()).build();
                                goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN.newBuilder()
                                                    .setFilename(entry.getValue().getFilename())
                                                    .setChunkId(entry.getValue().getChunkId())
                                                    .setSN(SN).build();
				logger.info("Sending good chunk info to SN hostname {} port {}",entry.getValue().getDataNode().getHostname(),
						entry.getValue().getDataNode().getPort());
                    		goodChunkInfoToSN.writeDelimitedTo(connectionSocket.getOutputStream());
				connectionSocket.close();
				break;
                            }
                        }
                     }
                }

            }
            catch (IOException e)
            {
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

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

    protected static final Logger logger = LoggerFactory.getLogger(Controller.class);
    protected static int controllerPort;
    protected static Map<String,DataNode> storageNodesList = new HashMap<>();
    protected static Map<String,Metadata> metadataMap = new HashMap<>();
    protected static Map<String,Boolean>  statusStorageNodesMap = new HashMap<>();
    protected static Map<Integer,String>  storageNodeMapToNum  = new HashMap<>();
    protected static Map<String,Long>     storageNodeHeartBeatTimeStamps = new HashMap<>();
    protected static final int NUM_THREADS_ALLOWED = 15;
    protected ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);
    protected static final Long MAX_ALLOWED_ACTIVENESS = 10000L;
    protected static String configPath = null;

    protected static final int REPLICATION_FACTOR = 3;

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
            executorService.submit(new Thread(new ControllerRequestHandler(socket)));
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
			            tasksToBeDoneAfterDeactivation(hostname,port);
                        break;
                    }
                }
            }
        }
    }

    private void tasksToBeDoneAfterDeactivation(String hostname,int port)
    {
        logger.info("setting status as false for host {} port {}",hostname,port);
        //remove all metadata related particular storage node
        String key1 = hostname+Integer.toString(port);
        statusStorageNodesMap.put(key1,false);
        storageNodesList.remove(key1);

        //get metadata of chunks hold by deactivated node
        List<Metadata> metadataOfDeActivatedNode = getMetadataOfDeActivatedNode(hostname,port);
        //nodes which have same replica;but this list has only one among them to pick one node to borrow data from that node!!!
        List<Metadata>replicaNodeMetadatas = new ArrayList<>();
        //datanodes which has the same replica!!
        List<DataNode> dataNodeList = new ArrayList<>();
        List<String> checkDuplicates = new ArrayList<>();

        for(Metadata metadata : metadataOfDeActivatedNode)
        {
            Iterator<Map.Entry<String,Metadata>> iterator = metadataMap.entrySet().iterator();
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
        logger.info("nodes which have same replica;but this list has only one among them to pick one node to borrow data from that node!!!");
        for (Metadata metadata : replicaNodeMetadatas)
        {
            logger.info("hostname {} port {}", metadata.getDataNode().getHostname(), metadata.getDataNode().getPort());
        }
        logger.info("datanodes which has the same replica!!");
        for(DataNode dataNode : dataNodeList)
        {
            logger.info("datanode {}",dataNode.toString());
        }

        //get the node where we want to send the replica of deactivated node
        DataNode replicaCopyToBeSentTo = getNodeToSendReplicaOfDeActivatedNode(dataNodeList);


        //send requests to storage nodes from where we want to get replica copy and send it to node which we picked
        //based on it is already having the same replica copy
        sendRequestsToSNForReplicaOfDeActivatedToSendToNewNode(replicaNodeMetadatas,replicaCopyToBeSentTo);
    }

    private List<Metadata> getMetadataOfDeActivatedNode(String hostname,int port)
    {
        String key = hostname + Integer.toString(port);
        Iterator<Map.Entry<String,Metadata>> iterator = metadataMap.entrySet().iterator();
        List<Metadata> metadatasOfDeactivated = new ArrayList<>();
        while (iterator.hasNext())
        {
            Map.Entry<String,Metadata> entry = iterator.next();
            if(entry.getKey().contains(key))
            {
                metadatasOfDeactivated.add(entry.getValue());
                iterator.remove();
            }
        }
        logger.info("deactivated {} {}",hostname,port);

        //Replica which are there in this node needs to be stored in some other storage node
        logger.info("Metadata of deactivated node");
        for(Metadata metadata : metadatasOfDeactivated)
        {
            logger.info("filename {} chunkid {}",metadata.getFilename(),metadata.getChunkId());
        }

        return metadatasOfDeactivated;
    }

    private DataNode getNodeToSendReplicaOfDeActivatedNode(List<DataNode> dataNodeList)
    {
        int count = 1;
        DataNode replicaCopyToBeSentTo = null;
        while(count <= 1)
        {
            Random r = new Random();
            int nodeNum = r.nextInt(storageNodeMapToNum.size())+1;
            logger.debug("node hostname {}",storageNodeMapToNum.get(nodeNum));
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
        return replicaCopyToBeSentTo;
    }

    private void sendRequestsToSNForReplicaOfDeActivatedToSendToNewNode(List<Metadata> metadatas,DataNode replicaToBeSentTo)
    {
        for(Metadata metadata : metadatas)
        {
            try
            {
                logger.info("Send Replica Copy to SN {} to port {}",replicaToBeSentTo.getHostname(), replicaToBeSentTo.getPort());
                Socket socket = new Socket(metadata.getDataNode().getHostname(),metadata.getDataNode().getPort());
                RequestsToStorageNode.SendReplicaCopyToSN.storageNode storageNode = RequestsToStorageNode.SendReplicaCopyToSN.storageNode.newBuilder()
                                                                                    .setHostname(replicaToBeSentTo.getHostname())
                                                                                    .setPort(replicaToBeSentTo.getPort()).build();
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
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    protected static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

}

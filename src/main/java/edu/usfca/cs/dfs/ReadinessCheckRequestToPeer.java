package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 10/16/17.
 */
public class ReadinessCheckRequestToPeer extends StorageNode implements Runnable {
    private RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSNMsg = null;
    public ReadinessCheckRequestToPeer(RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSNMsg)
    {
        this.readinessCheckRequestToSNMsg = readinessCheckRequestToSNMsg;
    }

    @Override
    public void run()
    {
        try
        {
            if(readinessCheckRequestToSNMsg.hasReadinessCheckRequestToSNFromClientMsg())
            {
                RequestsToStorageNode.ReadinessCheckRequestToSNFromClient requestToSNFromClient = readinessCheckRequestToSNMsg.getReadinessCheckRequestToSNFromClientMsg();

                if (requestToSNFromClient.getStorageNodeListList().size() > 0)
                {
                    List<RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode> peerList = requestToSNFromClient.getStorageNodeListList();
                    String filename = requestToSNFromClient.getFilename();
                    int chunkId = requestToSNFromClient.getChunkId();

                    String hostname = peerList.get(0).getHostname();
                    if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                    {
                        hostname = "Bhargavis-MacBook-Pro.local";
                    }
                    Socket socket = new Socket(hostname, peerList.get(0).getPort());
                    List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peers = new ArrayList<>();
                    if(peerList.size() > 1)
                    {
                        for (int i = 1; i < peerList.size(); i++) {
                            RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode storageNode = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode.newBuilder()
                                    .setPort(peerList.get(i).getPort())
                                    .setHostname(peerList.get(i).getHostname()).build();
                            peers.add(storageNode);
                        }
                    }
                    RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.Builder builder1 = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.newBuilder()
                            .addAllStorageNodeList(peers)
                            .setFilename(filename)
                            .setChunkId(chunkId);
                    RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                            .setReadinessCheckRequestToSNFromSNMsg(builder1);

                    RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                            .setReadinessCheckRequestToSNMsg(builder).build();
                    logger.info("Sending readiness check request to peer SN {} to port {} ",hostname,socket.getPort());
                    requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                    logger.info("Waiting for response from peer SN {} from port {}",hostname,socket.getPort());
                    ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                            .parseDelimitedFrom(socket.getInputStream());
                    logger.info("Received response from peer SN {} from port {}",hostname,socket.getPort());
                    socket.close();
                    String filepath = null;

                    filepath = dataDirectory + filename + "Part" + chunkId;

                    if (acknowledgeReadinessToSN.getSuccess()) {
                        Socket socket1 = new Socket(hostname, peerList.get(0).getPort());
                        File file = new File(filepath);
                        while(!file.exists())
                        {
                            Thread.sleep(1000);
                        }
                        logger.info("Store chunk request to peer SN {} to port {} ",hostname,socket1.getPort());
                        RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromSN.newBuilder()
                                .setFilename(filename)
                                .setChunkId(chunkId)
                                .setChunkData(ByteString.copyFrom(Files.readAllBytes(file.toPath())))
                                .build();
                        RequestsToStorageNode.StoreChunkRequestToSN requestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                .setStoreChunkRequestToSNFromSNMsg(storeChunkRequestToSN).build();
                        RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                .newBuilder()
                                .setStoreChunkRequestToSNMsg(requestToSN).build();
                        wrapper.writeDelimitedTo(socket1.getOutputStream());
                        ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN
                                .parseDelimitedFrom(socket1.getInputStream());
                        logger.info("Received store chunk response from peer SN {} from port {} ",hostname,socket1.getPort());
                        if (acknowledgeStoreChunkToSN.getSuccess())
                        {
                            logger.info("Store chunk..success in peer SN {} from port {} ",hostname,socket1.getPort());
                        }
                        socket1.close();
                    }
                }
            }

            if(readinessCheckRequestToSNMsg.hasReadinessCheckRequestToSNFromSNMsg())
            {
                RequestsToStorageNode.ReadinessCheckRequestToSNFromSN requestToSNFromSN = readinessCheckRequestToSNMsg.getReadinessCheckRequestToSNFromSNMsg();

                if (requestToSNFromSN.getStorageNodeListList().size() > 0)
                {
                    List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peerList = requestToSNFromSN.getStorageNodeListList();
                    String filename = requestToSNFromSN.getFilename();
                    int chunkId = requestToSNFromSN.getChunkId();

                    String hostname = peerList.get(0).getHostname();
                    if(hostname.contains("Bhargavis-MacBook-Pro.local"))
                    {
                        hostname = "Bhargavis-MacBook-Pro.local";
                    }
                    Socket socket = new Socket(hostname, peerList.get(0).getPort());
                    List<RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode> peers = new ArrayList<>();
                    if(peerList.size() > 1)
                    {
                        for (int i = 1; i < peerList.size(); i++) {
                            RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode storageNode = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.StorageNode.newBuilder()
                                    .setPort(peerList.get(i).getPort())
                                    .setHostname(peerList.get(i).getHostname()).build();
                            peers.add(storageNode);
                        }
                    }
                    RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.Builder builder1 = RequestsToStorageNode.ReadinessCheckRequestToSNFromSN.newBuilder()
                            .setFilename(filename)
                            .setChunkId(chunkId)
                            .addAllStorageNodeList(peers);
                    RequestsToStorageNode.ReadinessCheckRequestToSN.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                            .setReadinessCheckRequestToSNFromSNMsg(builder1);

                    RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                            .setReadinessCheckRequestToSNMsg(builder).build();
                    logger.info("Sending readiness check(SN-SN) request to peer SN {} to port {} ",hostname,socket.getPort());
                    requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());
                    logger.info("Waiting for readiness response from peer SN {} from port {}",hostname,socket.getPort());
                    ResponsesToStorageNode.AcknowledgeReadinessToSN acknowledgeReadinessToSN = ResponsesToStorageNode.AcknowledgeReadinessToSN
                            .parseDelimitedFrom(socket.getInputStream());
                    logger.info("Received readiness response(SN-SN) from peer SN {} from port {}",hostname,socket.getPort());
                    socket.close();
                    String filepath = null;

                    filepath = dataDirectory + filename + "Part" + chunkId;
                    if (acknowledgeReadinessToSN.getSuccess()) {
                        Socket socket1 = new Socket(hostname,peerList.get(0).getPort());
                        File file = new File(filepath);
                        while(!file.exists())
                        {
                            logger.debug("inside while sleeping till file is present looking for file {}",filepath);
                            Thread.sleep(1000);
                        }
                        logger.info("Store chunk request(SN-SN) to peer SN {} to port {} ",hostname,socket1.getPort());
                        RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromSN.newBuilder()
                                .setFilename(filename)
                                .setChunkId(chunkId)
                                .setChunkData(ByteString.copyFrom(Files.readAllBytes(file.toPath())))
                                .build();
                        RequestsToStorageNode.StoreChunkRequestToSN requestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                                .setStoreChunkRequestToSNFromSNMsg(storeChunkRequestToSN).build();
                        RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                .newBuilder()
                                .setStoreChunkRequestToSNMsg(requestToSN).build();
                        wrapper.writeDelimitedTo(socket1.getOutputStream());
                        ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN
                                .parseDelimitedFrom(socket1.getInputStream());
                        logger.info("Received store chunk(SN-SN) response from peer SN {} from port {}",hostname,socket1.getPort());
                        if (acknowledgeStoreChunkToSN.getSuccess())
                        {
                            logger.info("Store chunk..success in peer SN {} from port {}",hostname,socket1.getPort());
                        }
                        socket1.close();
                    }
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
        catch (InterruptedException e)
        {
            logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
        }
    }
}

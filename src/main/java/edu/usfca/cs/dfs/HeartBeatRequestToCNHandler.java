package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class HeartBeatRequestToCNHandler extends Controller {

    private RequestsToController.Heartbeat heartbeat;
    private Socket socket;

    public HeartBeatRequestToCNHandler(RequestsToController.Heartbeat heartbeat) {
        this.heartbeat = heartbeat;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void executeRequest() {
        try {
            //logger.debug("Received heartbeat message from storage node {} from port {}" , heartbeat.getSN().getHostname(),heartbeat.getSN().getPort());
            int size = heartbeat.getMetadataList().size();
            RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                    .setHostname(heartbeat.getSN().getHostname())
                    .setPort(heartbeat.getSN().getPort())
                    .setDiskCapacity(heartbeat.getSN().getDiskCapacity())
                    .setDiskSpaceUsed(heartbeat.getSN().getDiskSpaceUsed())
                    .build();
            DataNode dataNode = new DataNode(heartbeat.getSN().getPort(),heartbeat.getSN().getHostname());
            dataNode.setDiskCapacity(heartbeat.getSN().getDiskCapacity());
            dataNode.setDiskspaceUsed(heartbeat.getSN().getDiskSpaceUsed());
            storageNodesList.put(storageNode.getHostname() + storageNode.getPort(), dataNode);
            for (int i = 0; i < size; i++) {
                RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = heartbeat.getMetadataList().get(i);
                String key = chunkMetadata.getFilename() + chunkMetadata.getChunkId() + storageNode.getHostname() + storageNode.getPort();
                //logger.debug("metadata map key {}",key);
                if (!metadataMap.containsKey(key)) {
                    Metadata metadata = new Metadata(chunkMetadata.getFilename(), chunkMetadata.getChunkId());
                    DataNode dataNode1 = new DataNode(storageNode.getPort(), storageNode.getHostname());
                    dataNode1.setDiskCapacity(storageNode.getDiskCapacity());
                    dataNode1.setDiskspaceUsed(storageNode.getDiskSpaceUsed());
                    //logger.debug("disk space used {} total capacity {}",storageNode.getDiskSpaceUsed(),storageNode.getDiskCapacity());
                    metadata.setDataNode(dataNode1);
                    metadataMap.put(key, metadata);
                }
            }
            storageNodeHeartBeatTimeStamps.put(storageNode.getHostname() + storageNode.getPort(), System.currentTimeMillis());
            //logger.debug("Updated info from heartbeat message in memory from SN {} from port {}",msgWrapper.getHeartbeatMsg().getSN().getHostname(),msgWrapper.getHeartbeatMsg().getSN().getPort());
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

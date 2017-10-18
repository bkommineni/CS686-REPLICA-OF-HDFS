package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 10/17/17.
 */
public class StoreFile extends Client {
    private String filePath;

    public StoreFile(String filePath) {
        this.filePath = filePath;
    }

    public void executeRequest() {
        try {
            int filePart = 1;
            List<byte[]> blocks = chunking(filePath);
            String filename = filePath;
            String[] tokens = filename.split("/");
            filename = tokens[tokens.length - 1];

            for (byte[] block : blocks) {
                //sending block to Controller with blockInfo
                //StoreChunk request to Controller
                logger.info("Controller hostname {} Controller port {} ", controllerHostname, controllerPort);
                Socket socket = new Socket(controllerHostname, controllerPort);

                RequestsToController.StoreChunkRequest storeChunk = RequestsToController.StoreChunkRequest.newBuilder()
                        .setChunkId(filePart)
                        .setFilename(filename).build();
                RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                        .setStoreChunkRequestMsg(storeChunk).build();
                logger.info("Sending StoreChunk request to Controller {} to port {}", controllerHostname, controllerPort);
                requestsToControllerWrapper.writeDelimitedTo(socket.getOutputStream());

                logger.info("Waiting for StoreChunk response from Controller...");

                //Received response from Controller with list of three Storage Nodes to store the replicas
                ResponsesToClient.StoreChunkResponse response = ResponsesToClient.StoreChunkResponse.parseDelimitedFrom(socket.getInputStream());

                logger.info("Received StoreChunk response from Controller...");
                socket.close();

                //ReadinessCheck request to Storage Node-1
                String hostname = response.getStorageNodeList(0).getHostname();
                if (hostname.contains("Bhargavis-MacBook-Pro.local")) {
                    hostname = "Bhargavis-MacBook-Pro.local";
                }
                socket = new Socket(hostname, response.getStorageNodeList(0).getPort());

                List<RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode> storageNodeList = new ArrayList<>();
                for (int i = 1; i < response.getStorageNodeListList().size(); i++) {

                    RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode readinessCheck = RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeListList().get(i).getPort()).setHostname(response.getStorageNodeListList().get(i).getHostname()).build();
                    storageNodeList.add(readinessCheck);
                }
                RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.Builder builder = RequestsToStorageNode.ReadinessCheckRequestToSNFromClient.newBuilder()
                        .setFilename(filename)
                        .setChunkId(filePart)
                        .addAllStorageNodeList(storageNodeList);

                RequestsToStorageNode.ReadinessCheckRequestToSN requestToSN = RequestsToStorageNode.ReadinessCheckRequestToSN.newBuilder()
                        .setReadinessCheckRequestToSNFromClientMsg(builder).build();

                RequestsToStorageNode.RequestsToStorageNodeWrapper requestsToStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setReadinessCheckRequestToSNMsg(requestToSN).build();


                logger.info("Sending readinessCheck request to Storage Node {} to port {}", socket.getInetAddress(), socket.getPort());
                requestsToStorageNodeWrapper.writeDelimitedTo(socket.getOutputStream());

                //Received response from Storage Node-1 regarding Readiness Check
                ResponsesToClient.AcknowledgeReadinessToClient acknowledgeReadinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.parseDelimitedFrom(socket.getInputStream());

                logger.info("Received readinessCheck response from Storage Node...");

                socket.close();
                if (acknowledgeReadinessToClient.getSuccess()) {
                    //sends chunkMetadata and data to Storage Nodes in pipeline fashion for storage
                    //StoreChunkRequest to Storage Node
                    hostname = response.getStorageNodeList(0).getHostname();
                    if (hostname.contains("Bhargavis-MacBook-Pro.local")) {
                        hostname = "Bhargavis-MacBook-Pro.local";
                    }
                    socket = new Socket(hostname, response.getStorageNodeList(0).getPort());
                    RequestsToStorageNode.StoreChunkRequestToSNFromClient.StorageNode storageNode = RequestsToStorageNode.StoreChunkRequestToSNFromClient.StorageNode.newBuilder()
                            .setPort(response.getStorageNodeList(0).getPort()).build();


                    RequestsToStorageNode.StoreChunkRequestToSNFromClient storeChunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSNFromClient.newBuilder()
                            .addStorageNodeList(storageNode)
                            .setChunkId(filePart)
                            .setFilename(filename)
                            .setChunkData(ByteString.copyFrom(block)).build();

                    RequestsToStorageNode.StoreChunkRequestToSN chunkRequestToSN = RequestsToStorageNode.StoreChunkRequestToSN.newBuilder()
                            .setStoreChunkRequestToSNFromClientMsg(storeChunkRequestToSN).build();
                    RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                            .setStoreChunkRequestToSNMsg(chunkRequestToSN).build();

                    logger.info("Sending store chunk request to Storage Node {} to port {} ", socket.getInetAddress(), socket.getPort());
                    wrapper.writeDelimitedTo(socket.getOutputStream());
                    logger.info("Waiting for store chunk response from Storage Node...");

                    ResponsesToClient.ResponsesToClientWrapper responsesToClientWrapper = ResponsesToClient.ResponsesToClientWrapper.parseDelimitedFrom(socket.getInputStream());

                    if (responsesToClientWrapper.hasAcknowledgeStoreChunkToClientMsg()) {
                        if (responsesToClientWrapper.getAcknowledgeStoreChunkToClientMsg().getSuccess())
                            logger.info("Received response from Storage Node!!success");
                        else
                            logger.info("Received response from Storage Node!!fail");
                    }
                    socket.close();
                }
                filePart = filePart + 1;
            }
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

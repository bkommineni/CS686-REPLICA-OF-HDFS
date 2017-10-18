package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

public class ChunkRetrieveWorker extends Client implements Runnable {
    ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata;

    public ChunkRetrieveWorker(ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata) {
        this.chunkMetadata = chunkMetadata;
    }

    @Override
    public void run() {
        try {
            RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = RequestsToStorageNode.RetrieveFileRequestToSN.newBuilder()
                    .setChunkId(chunkMetadata.getChunkId())
                    .setFilename(chunkMetadata.getFilename())
                    .build();
            RequestsToStorageNode.RequestsToStorageNodeWrapper toStorageNodeWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                    .setRetrieveFileRequestToSNMsg(requestToSN).build();
            Socket socket1 = new Socket(chunkMetadata.getNode().getHostname(), chunkMetadata.getNode().getPort());
            logger.info("Sending RetrieveFile request to Storage Node {} to port {}", socket1.getInetAddress(), socket1.getPort());
            logger.info("Sending request for {} chunk {}", chunkMetadata.getFilename(), chunkMetadata.getChunkId());
            toStorageNodeWrapper.writeDelimitedTo(socket1.getOutputStream());
            logger.info("Waiting for RetrieveFile response from Storage Node...");


            ResponsesToClient.RetrieveFileResponseFromSN responseFromSN = ResponsesToClient.RetrieveFileResponseFromSN.parseDelimitedFrom(socket1.getInputStream());
            logger.info("Received RetrieveFile response from Storage Node...");
            byte[] temp = responseFromSN.getChunkData().toByteArray();
            String checksum = calculateChecksum(temp);
            String checksumDesired = responseFromSN.getChecksum();
            logger.info("checksum {} checksum desired {}", checksum, checksumDesired);

            //check the checksum
            while (!checksumDesired.equals(checksum)) {
                socket1.close();
                //checksum does not match
                //send request for right copy
                RequestsToStorageNode.SendGoodChunkRequestToSN goodChunkRequestToSN = RequestsToStorageNode.SendGoodChunkRequestToSN
                        .newBuilder()
                        .setFilename(chunkMetadata.getFilename())
                        .setChunkId(chunkMetadata.getChunkId())
                        .build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setSendGoodChunkRequestToSNMsg(goodChunkRequestToSN).build();
                socket1 = new Socket(chunkMetadata.getNode().getHostname(), chunkMetadata.getNode().getPort());
                logger.info("Sending good chunk request to SN {} to port {}", chunkMetadata.getNode().getHostname(), chunkMetadata.getNode().getPort());
                wrapper.writeDelimitedTo(socket1.getOutputStream());
                logger.info("Waiting for good chunk response from SN...");
                ResponsesToClient.GoodChunkDataToClient goodChunkData = ResponsesToClient.GoodChunkDataToClient
                        .parseDelimitedFrom(socket1.getInputStream());
                logger.info("Received good chunk data from SN {} from port {}", chunkMetadata.getNode().getHostname(), chunkMetadata.getNode().getPort());
                temp = goodChunkData.getChunkData().toByteArray();
                logger.debug("chunkdata {}", temp);
                checksum = calculateChecksum(temp);
                checksumDesired = goodChunkData.getChecksum();
                logger.info("checksumDesired {} checksum {}", checksumDesired, checksum);
            }
            listOfChunks.put(responseFromSN.getChunkId(), temp);
            logger.debug("list of chunks {}", listOfChunks);
            socket1.close();
        } catch (IOException e) {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}
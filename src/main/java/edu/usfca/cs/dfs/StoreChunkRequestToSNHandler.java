package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by bharu on 10/16/17.
 */
public class StoreChunkRequestToSNHandler extends StorageNode {

    private RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN;
    private Socket socket;

    public StoreChunkRequestToSNHandler(RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN)
    {
       this.storeChunkRequestToSN = storeChunkRequestToSN;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void executeRequest()
    {
        try
        {
            InetAddress inetAddress = socket.getInetAddress();
            int port = socket.getPort();
            String filename = null;
            int chunkId = 0;

            byte[] bytes = null;

            if (storeChunkRequestToSN.hasStoreChunkRequestToSNFromClientMsg())
            {
                logger.info("Received Store chunk request from Client {} from port {} ", inetAddress, port);
                RequestsToStorageNode.StoreChunkRequestToSNFromClient storeChunkRequestToSNFromClient = storeChunkRequestToSN.getStoreChunkRequestToSNFromClientMsg();
                filename = storeChunkRequestToSNFromClient.getFilename();
                chunkId = storeChunkRequestToSNFromClient.getChunkId();
                bytes = storeChunkRequestToSNFromClient.getChunkData().toByteArray();
            }
            else if (storeChunkRequestToSN.hasStoreChunkRequestToSNFromSNMsg())
            {
                logger.info("Received Store chunk request from SN {} from port {} ", inetAddress, port);
                RequestsToStorageNode.StoreChunkRequestToSNFromSN storeChunkRequestToSNFromSN = storeChunkRequestToSN.getStoreChunkRequestToSNFromSNMsg();
                filename = storeChunkRequestToSNFromSN.getFilename();
                chunkId = storeChunkRequestToSNFromSN.getChunkId();
                bytes = storeChunkRequestToSNFromSN.getChunkData().toByteArray();
            }

            /*Storing Chunk data on local file system of Node*/
            String blockFile = null;
            logger.info("filename {} ", filename);
            blockFile = dataDirectory + filename + "Part" + chunkId;
            File file = new File(blockFile);
            Files.deleteIfExists(Paths.get(blockFile));
            Files.createFile(Paths.get(blockFile));
            if(bytes.length < file.getFreeSpace())
            {
                Files.write(Paths.get(blockFile), bytes);
                logger.debug("file parent {}",file.getParent());
                logger.debug("free space {}",file.getFreeSpace());
                logger.debug("total space {}",file.getTotalSpace());
                diskSpaceUsed = (file.getTotalSpace()-file.getFreeSpace());

                /*Calculating Checksum and adding all the chunkInfo(filename,chunkId,Checksum) to metadata Map of Storage Node*/
                String checksum = calculateChecksum(blockFile);
                logger.info("checksum {}", checksum);
                StorageNodeMetadata metadata = new StorageNodeMetadata(filename, chunkId);
                metadata.setChecksum(checksum);
                String key = filename + Integer.toString(chunkId);
                storageNodeMetadataMap.put(key, metadata);
                dataStoredInLastFiveSeconds.put(key, metadata);

                if (storeChunkRequestToSN.hasStoreChunkRequestToSNFromClientMsg())
                {
                    ResponsesToClient.AcknowledgeStoreChunkToClient acknowledgeStoreChunkToClient = ResponsesToClient.AcknowledgeStoreChunkToClient.newBuilder()
                            .setSuccess(true).build();
                    ResponsesToClient.ResponsesToClientWrapper wrapper = ResponsesToClient.ResponsesToClientWrapper.newBuilder()
                            .setAcknowledgeStoreChunkToClientMsg(acknowledgeStoreChunkToClient).build();

                    wrapper.writeDelimitedTo(socket.getOutputStream());
                    logger.info("Store Chunk done and sent a response to client {} to port {} ", inetAddress, port);
                }
                if (storeChunkRequestToSN.hasStoreChunkRequestToSNFromSNMsg())
                {
                    ResponsesToStorageNode.AcknowledgeStoreChunkToSN acknowledgeStoreChunkToSN = ResponsesToStorageNode.AcknowledgeStoreChunkToSN.newBuilder()
                            .setSuccess(true).build();
                    ResponsesToStorageNode.ResponsesToStorageNodeWrapper wrapper = ResponsesToStorageNode.ResponsesToStorageNodeWrapper.newBuilder()
                            .setAcknowledgeStoreChunkToSNMsg(acknowledgeStoreChunkToSN).build();

                    wrapper.writeDelimitedTo(socket.getOutputStream());
                    logger.info("Store Chunk done and sent a response to SN {} to port {} ", inetAddress, port);
                }
                socket.close();
            }
            else
            {
                logger.error("File is too large to fit!!Not enough memory on disk!!");
            }
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
        }
    }
}

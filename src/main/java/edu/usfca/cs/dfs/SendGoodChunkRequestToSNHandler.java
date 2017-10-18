package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by bharu on 10/16/17.
 */
public class SendGoodChunkRequestToSNHandler extends StorageNode {

    RequestsToStorageNode.SendGoodChunkRequestToSN sendGoodChunkRequestToSN;
    private Socket socket;

    public SendGoodChunkRequestToSNHandler(RequestsToStorageNode.SendGoodChunkRequestToSN sendGoodChunkRequestToSN) {
        this.sendGoodChunkRequestToSN = sendGoodChunkRequestToSN;
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
            String f = dataDirectory + sendGoodChunkRequestToSN.getFilename() + "Part" + sendGoodChunkRequestToSN.getChunkId();
            logger.debug("corrupted data {}", Files.readAllBytes(Paths.get(f)));
            RequestsToController.SendGoodChunkRequest.storageNode storageNode = RequestsToController.SendGoodChunkRequest.storageNode.newBuilder()
                                                                                .setHostname(getHostname())
                                                                                .setPort(storageNodePort).build();
            //request to controller to send address of host which has same replica other than this
            RequestsToController.SendGoodChunkRequest sendGoodChunkRequest = RequestsToController.SendGoodChunkRequest.newBuilder()
                                                                            .setFilename(sendGoodChunkRequestToSN.getFilename())
                                                                            .setChunkId(sendGoodChunkRequestToSN.getChunkId())
                                                                            .setSN(storageNode).build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                                                                        .setSendGoodChunkRequestMsg(sendGoodChunkRequest).build();
            Socket controllerSocket = new Socket(controllerPortHostName, controllerPort);
            logger.info("Sending good chunk request to CN....");
            wrapper.writeDelimitedTo(controllerSocket.getOutputStream());
            ResponsesToStorageNode.GoodChunkInfoToSN goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN
                                                                        .parseDelimitedFrom(controllerSocket.getInputStream());
            logger.info("Received good chunk request from CN!!");
            controllerSocket.close();
            logger.info("good chunk is in host {} port {}", goodChunkInfoToSN.getSN().getHostname(), goodChunkInfoToSN.getSN().getPort());
            Socket snSocket = new Socket(goodChunkInfoToSN.getSN().getHostname(), goodChunkInfoToSN.getSN().getPort());
            RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.storageNode SN = RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.storageNode
                                                                                    .newBuilder()
                                                                                    .setHostname(getHostname())
                                                                                    .setPort(storageNodePort).build();
            RequestsToStorageNode.SendGoodChunkRequestFromSNToSN snToSN = RequestsToStorageNode.SendGoodChunkRequestFromSNToSN.newBuilder()
                                                                            .setSN(SN)
                                                                            .setFilename(goodChunkInfoToSN.getFilename())
                                                                            .setChunkId(goodChunkInfoToSN.getChunkId()).build();
            RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper1 = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                                                                            .setSendGoodChunkRequestFromSNToSNMsg(snToSN).build();
            logger.info("Sending good chunk request to SN {} from port {}", goodChunkInfoToSN.getSN().getHostname(), goodChunkInfoToSN.getSN().getPort());
            wrapper1.writeDelimitedTo(snSocket.getOutputStream());
            ResponsesToStorageNode.GoodChunkDataToSN chunkDataToSN = ResponsesToStorageNode.GoodChunkDataToSN
                                                                    .parseDelimitedFrom(snSocket.getInputStream());
            logger.info("Received good chunk data from SN ");
            snSocket.close();

            String filename = chunkDataToSN.getFilename();
            int chunkId = chunkDataToSN.getChunkId();
            byte[] chunkData = chunkDataToSN.getChunkData().toByteArray();
            replaceWithGoodChunk(chunkData,filename,chunkId);

            //send good chunk back to client which is waiting
            String filePath = dataDirectory + filename + "Part" + chunkId;
            BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
            String line = null;
            while ((line = reader.readLine()) != null)
            {
                logger.debug("file info {}", line);
            }
            StorageNodeMetadata storageNodeMetadata = storageNodeMetadataMap.get(filename+chunkId);
            ResponsesToClient.GoodChunkDataToClient goodChunkData = ResponsesToClient.GoodChunkDataToClient.newBuilder()
                                                                    .setChunkData(ByteString.copyFrom(Files.readAllBytes(Paths.get(filePath))))
                                                                    .setChecksum(storageNodeMetadata.getChecksum())
                                                                    .setFilename(chunkDataToSN.getFilename())
                                                                    .setChunkId(chunkDataToSN.getChunkId()).build();
            logger.info("Sending good chunk data back to client!!!");
            goodChunkData.writeDelimitedTo(socket.getOutputStream());
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }

    private void replaceWithGoodChunk(byte[] bytes,String filename,int chunkId)
    {
        try
        {
            //replace the chunk
            String filePath = dataDirectory + filename + "Part" + chunkId;
            File file = new File(filePath);

            logger.info("chunkdata {}", bytes);
            logger.info("free space {} bytes size {}",file.getFreeSpace(),bytes.length);
            if(bytes.length < file.getFreeSpace())
            {
                Files.deleteIfExists(Paths.get(filePath));
                Files.write(Paths.get(filePath), bytes);
                diskSpaceUsed = (file.getTotalSpace()-file.getFreeSpace());
                //replace checksum in the entry of metadata
                String checksum = calculateChecksum(filePath);
                logger.info("checksum {}", checksum);
                StorageNodeMetadata metadata = new StorageNodeMetadata(filename,chunkId);
                metadata.setChecksum(checksum);
                storageNodeMetadataMap.put(filename + chunkId, metadata);
                logger.info("good chunkData {}", Files.readAllBytes(Paths.get(filePath)));
            }
            else
            {
                logger.error("File is too large to fit!!Not enough memory on disk!!");
            }
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }


}

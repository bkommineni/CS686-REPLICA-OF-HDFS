package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by bharu on 10/16/17.
 */
public class StorageNodeRequestHandler extends StorageNode implements Runnable
{
    private Socket socket;

    public StorageNodeRequestHandler(Socket socket)
    {
        this.socket = socket;
    }

    @Override
    public void run()
    {
        try
        {
            logger.info("socket {} {}",socket.getInetAddress(),socket.getPort());
            logger.info("before reading a request...");
            RequestsToStorageNode.RequestsToStorageNodeWrapper requestsWrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper
                    .parseDelimitedFrom(socket.getInputStream());

            logger.info("received a request..");
            logger.info("data directory {}",dataDirectory);
            logger.info("controller port {}",controllerPort);
            logger.info("controller port hostname {}",controllerPortHostName);

            if(requestsWrapper.hasStoreChunkRequestToSNMsg())
            {
                //Process the Request
                logger.info("store request");
                RequestsToStorageNode.StoreChunkRequestToSN storeChunkRequestToSN = requestsWrapper.getStoreChunkRequestToSNMsg();
                StoreChunkRequestToSNHandler handler = new StoreChunkRequestToSNHandler(storeChunkRequestToSN);
                handler.setSocket(socket);
                handler.executeRequest();
            }

            if(requestsWrapper.hasRetrieveFileRequestToSNMsg())
            {
                RequestsToStorageNode.RetrieveFileRequestToSN requestToSN = requestsWrapper.getRetrieveFileRequestToSNMsg();
                RetrieveFileRequestToSNHandler handler = new RetrieveFileRequestToSNHandler(requestToSN);
                handler.setSocket(socket);
                handler.executeRequest();
            }

            if(requestsWrapper.hasReadinessCheckRequestToSNMsg())
            {
                logger.info("readiness check request..");
                RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN = requestsWrapper.getReadinessCheckRequestToSNMsg();
                ReadinessCheckRequestToSNHandler handler = new ReadinessCheckRequestToSNHandler(readinessCheckRequestToSN);
                handler.setSocket(socket);
                handler.executeRequest();
            }
            if(requestsWrapper.hasSendGoodChunkRequestToSNMsg())
            {
                logger.info("Received good chunk request from Client!!");
                RequestsToStorageNode.SendGoodChunkRequestToSN sendGoodChunkRequestToSN = requestsWrapper.getSendGoodChunkRequestToSNMsg();
                SendGoodChunkRequestToSNHandler handler = new SendGoodChunkRequestToSNHandler(sendGoodChunkRequestToSN);
                handler.setSocket(socket);
                handler.executeRequest();
            }
            if(requestsWrapper.hasSendGoodChunkRequestFromSNToSNMsg())
            {
                RequestsToStorageNode.SendGoodChunkRequestFromSNToSN SN = requestsWrapper.getSendGoodChunkRequestFromSNToSNMsg();
                logger.info("Received good chunk data req from peer SN {} from port {} for file {} chunk {}",SN.getSN().getHostname()
                        ,SN.getSN().getPort(),SN.getFilename(),SN.getChunkId());
                String filePath = dataDirectory + SN.getFilename()+"Part"+ SN.getChunkId();
                byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
                logger.info("chunkData {}",chunkData);
                ResponsesToStorageNode.GoodChunkDataToSN chunkDataToSN = ResponsesToStorageNode.GoodChunkDataToSN.newBuilder()
                                                                        .setChunkData(ByteString.copyFrom(chunkData))
                                                                        .setFilename(SN.getFilename())
                                                                        .setChunkId(SN.getChunkId()).build();
                chunkDataToSN.writeDelimitedTo(socket.getOutputStream());
                logger.info("Sending response with good chunk data..");
                socket.close();
            }
            if(requestsWrapper.hasSendReplicaCopyToSNMsg())
            {
                RequestsToStorageNode.SendReplicaCopyToSN replicaCopyToSN = requestsWrapper.getSendReplicaCopyToSNMsg();
                logger.info("Received send replica copy request from controller...");
                //get that file chunk data based on sent info
                String filePath = dataDirectory + replicaCopyToSN.getFilename()+ "Part" + replicaCopyToSN.getChunkId();
                byte[] chunkData = Files.readAllBytes(Paths.get(filePath));
                RequestsToStorageNode.SendReplicaCopyToSNFromSN replicaCopyToSNFromSN = RequestsToStorageNode.SendReplicaCopyToSNFromSN.newBuilder()
                        .setChunkData(ByteString.copyFrom(chunkData))
                        .setFilename(replicaCopyToSN.getFilename())
                        .setChunkId(replicaCopyToSN.getChunkId()).build();
                Socket socket = new Socket(replicaCopyToSN.getSN().getHostname(),replicaCopyToSN.getSN().getPort());
                RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper = RequestsToStorageNode.RequestsToStorageNodeWrapper.newBuilder()
                        .setSendReplicaCopyToSNFromSNMsg(replicaCopyToSNFromSN).build();
                logger.info("Sent replica copy to SN hostname {} port {}",replicaCopyToSN.getSN().getHostname(),replicaCopyToSN.getSN().getPort());
                wrapper.writeDelimitedTo(socket.getOutputStream());
                socket.close();
            }
            if(requestsWrapper.hasSendReplicaCopyToSNFromSNMsg())
            {
                logger.info("Received replica store request from SN..");
                RequestsToStorageNode.SendReplicaCopyToSNFromSN replicaCopyToSNFromSN = requestsWrapper.getSendReplicaCopyToSNFromSNMsg();

                String fileName = replicaCopyToSNFromSN.getFilename();
                int chunkId = replicaCopyToSNFromSN.getChunkId();
                String filePath = dataDirectory + fileName +"Part"+ chunkId;
                Files.createFile(Paths.get(filePath));
                Files.write(Paths.get(filePath),replicaCopyToSNFromSN.getChunkData().toByteArray());
                File file = new File(filePath);
                diskSpaceUsed = (file.getTotalSpace()-file.getFreeSpace());
                //checksum
                String checksum = calculateChecksum(filePath);
                logger.info("checksum {}",checksum);
                StorageNodeMetadata metadata = new StorageNodeMetadata(fileName,chunkId);
                metadata.setChecksum(checksum);
                storageNodeMetadataMap.put(fileName+chunkId,metadata);
                dataStoredInLastFiveSeconds.put(fileName+chunkId,metadata);
                logger.info("Stored replica on my storage node..");
            }
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}",ExceptionUtils.getStackTrace(e));
        }
    }
}

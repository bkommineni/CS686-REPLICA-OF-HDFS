package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;

/**
 * Created by bharu on 10/16/17.
 */
public class RetrieveFileRequestToSNHandler extends StorageNode {

    RequestsToStorageNode.RetrieveFileRequestToSN requestToSN;
    private Socket socket ;

    public RetrieveFileRequestToSNHandler(RequestsToStorageNode.RetrieveFileRequestToSN requestToSN)
    {
        this.requestToSN = requestToSN;
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
            String filename = requestToSN.getFilename();
            byte[] chunkData = null;

            logger.info("Received Retrieve file request from Client {} from port {}", inetAddress, port);
            chunkData = Files.readAllBytes(new File(dataDirectory + filename + "Part" + requestToSN.getChunkId()).toPath());
            StorageNodeMetadata storageNodeMetadata = storageNodeMetadataMap.get(requestToSN.getFilename() + requestToSN.getChunkId());
            logger.debug("chunk data {}", chunkData);
            ResponsesToClient.RetrieveFileResponseFromSN response = ResponsesToClient.RetrieveFileResponseFromSN.newBuilder()
                    .setChecksum(storageNodeMetadata.getChecksum())
                    .setFilename(requestToSN.getFilename())
                    .setChunkId(requestToSN.getChunkId())
                    .setChunkData(ByteString.copyFrom(chunkData)).build();
            response.writeDelimitedTo(socket.getOutputStream());
            logger.info("Retrieve chunk from this node is done and sent back response");
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

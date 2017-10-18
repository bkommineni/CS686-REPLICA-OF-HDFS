package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 10/17/17.
 */
public class RetrieveFileRequestToCNHandler extends Controller {

    private RequestsToController.RetrieveFileRequest retrieve;
    private Socket socket;

    public RetrieveFileRequestToCNHandler(RequestsToController.RetrieveFileRequest retrieve) {
        this.retrieve = retrieve;
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

            logger.info("Received retrieve file request from client {} from port {}", inetAddress, port);
            List<Metadata> metadatas = new ArrayList<>();
            String filename = retrieve.getFilename();
            logger.debug("retrieve file request filename {}", filename);
            for (String str : metadataMap.keySet()) {
                logger.debug("metadata map key {}", str);
                if (str.contains(filename)) {
                    Metadata metadata = metadataMap.get(str);
                    String hostname = metadata.getDataNode().getHostname();
                    logger.debug("hostname {} port {}", metadata.getDataNode().getHostname(), metadata.getDataNode().getPort());
                    if (hostname.contains("Bhargavis-MacBook-Pro.local")) {
                        if (statusStorageNodesMap.get(metadata.getDataNode().getHostname() + metadata.getDataNode().getPort())) {
                            logger.debug("status checked!!");
                            if (!metadatas.contains(metadata)) {
                                metadatas.add(metadataMap.get(str));
                            }
                        }
                    } else {
                        if (statusStorageNodesMap.get(metadata.getDataNode().getHostname())) {
                            logger.debug("status checked!!");
                            if (!metadatas.contains(metadata)) {
                                metadatas.add(metadataMap.get(str));
                            }
                        }
                    }
                }
            }
            logger.info("metadatas {}", metadatas);

            List<ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata> chunkMetadatas = new ArrayList<>();
            for (Metadata metadata : metadatas) {
                ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.storageNode storageNode =
                        ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.storageNode.newBuilder()
                                .setPort(metadata.getDataNode().getPort())
                                .setHostname(metadata.getDataNode().getHostname())
                                .build();
                ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata =
                        ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata.newBuilder()
                                .setChunkId(metadata.getChunkId())
                                .setFilename(retrieve.getFilename())
                                .setNode(storageNode)
                                .build();
                chunkMetadatas.add(chunkMetadata);
            }
            ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN
                    .newBuilder()
                    .addAllChunkList(chunkMetadatas)
                    .build();
            responseFromCN.writeDelimitedTo(socket.getOutputStream());
            logger.info("Responded with list of three distinct storage nodes to client for retrieve file request");
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;

/**
 * Created by bharu on 10/17/17.
 */
public class SendGoodChunkRequestToCNHandler extends Controller {
    private RequestsToController.SendGoodChunkRequest goodChunkRequest;
    private Socket socket;

    public SendGoodChunkRequestToCNHandler(RequestsToController.SendGoodChunkRequest goodChunkRequest) {
        this.goodChunkRequest = goodChunkRequest;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void executeRequest()
    {
        try {
            ResponsesToStorageNode.GoodChunkInfoToSN goodChunkInfoToSN = null;
            ResponsesToStorageNode.GoodChunkInfoToSN.storageNode SN = null;
            String key = goodChunkRequest.getFilename() + goodChunkRequest.getChunkId();
            for (Map.Entry<String, Metadata> entry : metadataMap.entrySet()) {
                logger.info("map entry {} protobuf msg {}", entry.getValue().getDataNode().getHostname(), goodChunkRequest.getSN().getHostname());
                logger.info("map entry port {} protobuf msg port {}", entry.getValue().getDataNode().getPort(), goodChunkRequest.getSN().getPort());
                if (entry.getKey().contains(key))
                {
                    if (goodChunkRequest.getSN().getHostname().equals("Bhargavis-MacBook-Pro.local"))
                    {
                        if (entry.getValue().getDataNode().getPort() != goodChunkRequest.getSN().getPort())
                        {
                            SN = ResponsesToStorageNode.GoodChunkInfoToSN.storageNode.newBuilder()
                                    .setHostname(entry.getValue().getDataNode().getHostname())
                                    .setPort(entry.getValue().getDataNode().getPort()).build();
                            goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN.newBuilder()
                                    .setFilename(entry.getValue().getFilename())
                                    .setChunkId(entry.getValue().getChunkId())
                                    .setSN(SN).build();
                            logger.info("Sending good chunk info to SN hostname {} port {}", entry.getValue().getDataNode().getHostname(),
                                    entry.getValue().getDataNode().getPort());
                            goodChunkInfoToSN.writeDelimitedTo(socket.getOutputStream());
                            socket.close();
                            break;
                        }
                    }
                    else if (!entry.getValue().getDataNode().getHostname().equals(goodChunkRequest.getSN().getHostname()))
                    {
                        SN = ResponsesToStorageNode.GoodChunkInfoToSN.storageNode.newBuilder()
                                .setHostname(entry.getValue().getDataNode().getHostname())
                                .setPort(entry.getValue().getDataNode().getPort()).build();
                        goodChunkInfoToSN = ResponsesToStorageNode.GoodChunkInfoToSN.newBuilder()
                                .setFilename(entry.getValue().getFilename())
                                .setChunkId(entry.getValue().getChunkId())
                                .setSN(SN).build();
                        logger.info("Sending good chunk info to SN hostname {} port {}", entry.getValue().getDataNode().getHostname(),
                                entry.getValue().getDataNode().getPort());
                        goodChunkInfoToSN.writeDelimitedTo(socket.getOutputStream());
                        socket.close();
                        break;
                    }
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

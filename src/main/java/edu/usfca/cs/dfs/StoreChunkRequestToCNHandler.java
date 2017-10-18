package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by bharu on 10/17/17.
 */
public class StoreChunkRequestToCNHandler extends Controller {
    private RequestsToController.StoreChunkRequest store;
    private Socket socket;

    public StoreChunkRequestToCNHandler(RequestsToController.StoreChunkRequest store) {
        this.store = store;
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

            logger.info("Received store chunk request from client {} from port {}", inetAddress, port);
            List<ResponsesToClient.StoreChunkResponse.storageNode> storageNodes = new ArrayList<>();

            int count = 1;
            List<Integer> nodenums = new ArrayList<>();
            for (int num : storageNodeMapToNum.keySet()) {
                logger.debug(storageNodeMapToNum.get(num));
            }
            while (count <= REPLICATION_FACTOR) {
                Random r = new Random();
                int nodeNum = r.nextInt(storageNodeMapToNum.size()) + 1;
                logger.debug("nodenum {}", nodeNum);
                if (storageNodeMapToNum.get(nodeNum) != null) {
                    logger.debug("if loop...." + nodeNum + "--" + statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)) + "--" + nodenums.contains(nodeNum));
                    logger.debug("status of storageNode {} list check {} on host {}", statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)), nodenums.contains(nodeNum), storageNodeMapToNum.get(nodeNum));
                    if (statusStorageNodesMap.get(storageNodeMapToNum.get(nodeNum)) && (!nodenums.contains(nodeNum))) {
                        logger.info("Replica Node Number {} Replica Node hostname {} ", nodeNum, storageNodeMapToNum.get(nodeNum));
                        DataNode storageNode = storageNodesList.get(storageNodeMapToNum.get(nodeNum));
                        ResponsesToClient.StoreChunkResponse.storageNode storageNodeMsg =
                                ResponsesToClient.StoreChunkResponse.storageNode.newBuilder().setPort(storageNode.getPort())
                                        .setHostname(storageNode.getHostname())
                                        .build();
                        storageNodes.add(storageNodeMsg);
                        nodenums.add(nodeNum);
                        count++;
                    }
                }
            }

            //return the set of nodes/numbers randomly generated in response
            ResponsesToClient.StoreChunkResponse.Builder builder = ResponsesToClient.StoreChunkResponse.newBuilder();
            ResponsesToClient.StoreChunkResponse storeChunkResponse = builder.addAllStorageNodeList(storageNodes).build();
            storeChunkResponse.writeDelimitedTo(socket.getOutputStream());
            logger.info("Responded with list of three distinct storage nodes to client for store chunk request");
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

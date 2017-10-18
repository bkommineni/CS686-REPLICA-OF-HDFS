package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 10/18/17.
 */
public class ListActiveNodesToCNHandler extends Controller {

    private RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest;
    private Socket socket;

    public ListActiveNodesToCNHandler(RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest) {
        this.listOfActiveNodesRequest = listOfActiveNodesRequest;
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
            InetAddress inetAddress = socket.getInetAddress();
            int port = socket.getPort();
            logger.info("Received request for list of files on different nodes from client {} from port {}", inetAddress, port);
            List<ResponsesToClient.ListOfActiveStorageNodesFromCN.storageNode> storageNodes = new ArrayList<>();
            for (String str : statusStorageNodesMap.keySet()) {
                if (statusStorageNodesMap.get(str)) {
                    List<DataNode> dataNodeList = new ArrayList<>();
                    for (String key : metadataMap.keySet()) {
                        if (key.contains(str)) {
                            DataNode dataNode = metadataMap.get(key).getDataNode();
                            if (!dataNodeList.contains(dataNode)) {
                                dataNodeList.add(dataNode);
                                DataNode storageNode = metadataMap.get(key).getDataNode();
                                ResponsesToClient.ListOfActiveStorageNodesFromCN.storageNode SN =
                                        ResponsesToClient.ListOfActiveStorageNodesFromCN.storageNode.newBuilder()
                                                .setPort(storageNode.getPort())
                                                .setHostname(storageNode.getHostname())
                                                .build();
                                storageNodes.add(SN);
                            }
                        }
                    }
                }
            }
            ResponsesToClient.ListOfActiveStorageNodesFromCN list = ResponsesToClient.ListOfActiveStorageNodesFromCN.newBuilder()
                    .addAllSN(storageNodes).build();
            list.writeDelimitedTo(socket.getOutputStream());
            logger.info("Responded with a list of active storage nodes");
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

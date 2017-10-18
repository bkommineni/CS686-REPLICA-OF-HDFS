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
public class ListRequestToCNHandler extends Controller {
    private RequestsToController.ListOfActiveNodesRequest list;
    private Socket socket;

    public ListRequestToCNHandler(RequestsToController.ListOfActiveNodesRequest list) {
        this.list = list;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void executeRequest() {
        try {
            InetAddress inetAddress = socket.getInetAddress();
            int port = socket.getPort();
            logger.info("Received request for list of active storage nodes from client {} from port {}", inetAddress, port);
            List<ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo> storageNodes = new ArrayList<>();
            for (String str : statusStorageNodesMap.keySet()) {
                if (statusStorageNodesMap.get(str)) {
                    List<String> filenames = new ArrayList<>();
                    for (String key : metadataMap.keySet()) {
                        if (key.contains(str)) {
                            String filename = metadataMap.get(key).getFilename();
                            if (!filenames.contains(filename)) {
                                filenames.add(filename);
                                DataNode storageNode = metadataMap.get(key).getDataNode();
                                ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo storageNodeMsg =
                                        ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo.newBuilder().setPort(storageNode.getPort())
                                                .setHostname(storageNode.getHostname())
                                                .setFilename(metadataMap.get(key).getFilename())
                                                .build();
                                storageNodes.add(storageNodeMsg);
                            }
                        }
                    }

                }
            }
            ResponsesToClient.ListOfActiveStorageNodesResponseFromCN list = ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.newBuilder()
                    .addAllActiveStorageNodes(storageNodes).build();
            list.writeDelimitedTo(socket.getOutputStream());
            logger.info("Responded with a list of active storage nodes");
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

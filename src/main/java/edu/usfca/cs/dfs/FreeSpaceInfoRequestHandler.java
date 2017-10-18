package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class FreeSpaceInfoRequestHandler extends Controller {
    private RequestsToController.FreeSpaceInfoRequest freeSpaceInfoRequest;
    private Socket socket;

    public FreeSpaceInfoRequestHandler(RequestsToController.FreeSpaceInfoRequest freeSpaceInfoRequest) {
        this.freeSpaceInfoRequest = freeSpaceInfoRequest;
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
            logger.info("Received request for free space from client {} from port {}", inetAddress, port);
            long totalDiskSpaceOnCluster = 0;
            long totalUsedSpaceOnCluster = 0;

            for (String str : storageNodesList.keySet()) {
                totalDiskSpaceOnCluster = totalDiskSpaceOnCluster + storageNodesList.get(str).getDiskCapacity();
                totalUsedSpaceOnCluster = totalUsedSpaceOnCluster + storageNodesList.get(str).getDiskspaceUsed();
            }
            logger.info("total space {} total used {}", totalDiskSpaceOnCluster, totalUsedSpaceOnCluster);
            long availableDiskSpaceOnCluster = (totalDiskSpaceOnCluster - totalUsedSpaceOnCluster);
            ResponsesToClient.FreeSpaceInfoResponseFromCN freeSpaceInfoResponseFromCN = ResponsesToClient.FreeSpaceInfoResponseFromCN.newBuilder()
                    .setClusterAvailableSpace(availableDiskSpaceOnCluster)
                    .setClusterCapacity(totalDiskSpaceOnCluster)
                    .setClusterUsedSpace(totalUsedSpaceOnCluster).build();
            freeSpaceInfoResponseFromCN.writeDelimitedTo(socket.getOutputStream());
            logger.info("Responded with free space on cluster");
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

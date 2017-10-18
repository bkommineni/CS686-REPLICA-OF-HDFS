package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by bharu on 10/16/17.
 */
public class ReadinessCheckRequestToSNHandler extends StorageNode {

    private RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN;
    private Socket socket;

    public ReadinessCheckRequestToSNHandler(RequestsToStorageNode.ReadinessCheckRequestToSN readinessCheckRequestToSN) {
        this.readinessCheckRequestToSN = readinessCheckRequestToSN;
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
            if (readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromClientMsg()) {
                logger.info("Received readiness check request from client {} from port {} ", inetAddress, port);
                ResponsesToClient.AcknowledgeReadinessToClient readinessToClient = ResponsesToClient.AcknowledgeReadinessToClient.newBuilder()
                        .setSuccess(true).build();
                readinessToClient.writeDelimitedTo(socket.getOutputStream());
                logger.info("Sent acknowledgement for readiness check to client");
                logger.info("list of nodes {}", readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList());
                if (readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList().size() > 0) {
                    logger.info("Another Thread which sends request to other storage nodes in list ");
                    executorService.submit(new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)));
                }
            }

            if (readinessCheckRequestToSN.hasReadinessCheckRequestToSNFromSNMsg()) {
                logger.info("Received readiness check request from Storage Node {} from port {} ", inetAddress, port);
                ResponsesToStorageNode.AcknowledgeReadinessToSN readinessToClient = ResponsesToStorageNode.AcknowledgeReadinessToSN.newBuilder()
                        .setSuccess(true).build();
                readinessToClient.writeDelimitedTo(socket.getOutputStream());
                logger.info("Sent acknowledgement for readiness check to Storage Node");
                logger.info("list of nodes {}", readinessCheckRequestToSN.getReadinessCheckRequestToSNFromClientMsg().getStorageNodeListList());
                if (readinessCheckRequestToSN.getReadinessCheckRequestToSNFromSNMsg().getStorageNodeListList().size() > 0) {
                    logger.info("Another Thread which sends request to other storage nodes in list ");
                    executorService.submit(new Thread(new ReadinessCheckRequestToPeer(readinessCheckRequestToSN)));
                }
            }
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

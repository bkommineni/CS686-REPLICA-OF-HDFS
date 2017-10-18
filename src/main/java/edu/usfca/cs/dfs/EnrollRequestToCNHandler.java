package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class EnrollRequestToCNHandler extends Controller {
    private Socket socket;
    private RequestsToController.Enroll enroll;

    public EnrollRequestToCNHandler(RequestsToController.Enroll enroll) {
        this.enroll = enroll;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void executeRequest() {
        try {
            String hostname = enroll.getHostname();
            logger.info("Received enrollment request from storage node {} from port {}", hostname, enroll.getPort());

            //setting storage node to active
            if (hostname.contains("Bhargavis-MacBook-Pro.local")) {
                statusStorageNodesMap.put(hostname + enroll.getPort(), true);
                storageNodesList.put(hostname + enroll.getPort(),
                        new DataNode(enroll.getPort(), enroll.getHostname()));
            } else {
                logger.debug("enrolling and setting status to true..");
                statusStorageNodesMap.put(hostname, true);
                logger.debug("status after enrolling {} of host {}", statusStorageNodesMap.get(hostname), hostname);
                storageNodesList.put(hostname,
                        new DataNode(enroll.getPort(), enroll.getHostname()));
            }

            ResponsesToStorageNode.AcknowledgeEnrollment acknowledgeEnrollment = ResponsesToStorageNode.AcknowledgeEnrollment
                    .newBuilder().setSuccess(true).build();
            logger.info("enrolled host : {}", hostname);
            acknowledgeEnrollment.writeDelimitedTo(socket.getOutputStream());
            logger.info("Enrollment done!And acknowedged storage node with response");
            executorService.submit(new Thread(new ActivenessChecker(hostname, enroll.getPort())));
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

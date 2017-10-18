package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class ListActiveNodes extends Client {
    public void executeRequest() {
        try {
            Socket socket = new Socket(controllerHostname, controllerPort);
            RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest = RequestsToController.ListOfActiveNodesRequest
                    .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                    .newBuilder()
                    .setListOfActiveNodesRequestMsg(listOfActiveNodesRequest)
                    .build();
            logger.info("Sending  a list of active nodes request to Controller...");
            wrapper.writeDelimitedTo(socket.getOutputStream());
            ResponsesToClient.ListOfActiveStorageNodesFromCN list = ResponsesToClient.ListOfActiveStorageNodesFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received response from controller");
            for (ResponsesToClient.ListOfActiveStorageNodesFromCN.storageNode info : list.getSNList()) {
                logger.info("Host: {} Port: {}",info.getHostname(), info.getPort());
            }
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

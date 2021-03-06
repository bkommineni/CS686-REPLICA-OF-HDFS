package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/18/17.
 */
public class ListFiles extends Client {
    public void executeRequest() {
        try {
            Socket socket = new Socket(controllerHostname, controllerPort);
            RequestsToController.ListOfFilesOnNodesRequest listOfFilesOnNodesRequest = RequestsToController.ListOfFilesOnNodesRequest
                    .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                    .newBuilder()
                    .setListOfFilesOnNodesRequestMsg(listOfFilesOnNodesRequest)
                    .build();
            logger.info("Sending  a list of files request to Controller...");
            wrapper.writeDelimitedTo(socket.getOutputStream());
            ResponsesToClient.ListOfFilesOnNodesResponseFromCN list = ResponsesToClient.ListOfFilesOnNodesResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received response from controller");
            for (ResponsesToClient.ListOfFilesOnNodesResponseFromCN.storageNodeFileInfo info : list.getListOfStorageNodesWithFileInfoList()) {
                logger.info("filename: {} Host: {} Port: {}",info.getFilename(),info.getHostname(), info.getPort());
            }
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

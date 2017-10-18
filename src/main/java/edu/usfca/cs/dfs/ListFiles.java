package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class ListFiles extends Client
{
    public void executeRequest()
    {
        try
        {
            Socket socket = new Socket(controllerHostname, controllerPort);
            RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest = RequestsToController.ListOfActiveNodesRequest
                                                                                    .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                                                                        .newBuilder()
                                                                        .setListOfActiveNodes(listOfActiveNodesRequest)
                                                                        .build();
            logger.info("Sending  a list request to Controller...");
            wrapper.writeDelimitedTo(socket.getOutputStream());
            ResponsesToClient.ListOfActiveStorageNodesResponseFromCN list = ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received response from controller");
            for (ResponsesToClient.ListOfActiveStorageNodesResponseFromCN.storageNodeFileInfo info : list.getActiveStorageNodesList())
            {
                logger.info("Filename: {} Host: {} Port: {}", info.getFilename(), info.getHostname(), info.getPort());
            }
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

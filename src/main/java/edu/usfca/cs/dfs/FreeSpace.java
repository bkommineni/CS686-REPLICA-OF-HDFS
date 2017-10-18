package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class FreeSpace extends Client
{
    public void executeRequest()
    {
        try
        {
            Socket socket = new Socket(controllerHostname, controllerPort);
            RequestsToController.FreeSpaceInfoRequest freeSpaceInfoRequest = RequestsToController.FreeSpaceInfoRequest
                                                                            .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                                                                        .newBuilder()
                                                                        .setFreeSpaceInfoRequestMsg(freeSpaceInfoRequest)
                                                                        .build();
            logger.info("Sending  a free space request to Controller...");
            wrapper.writeDelimitedTo(socket.getOutputStream());
            ResponsesToClient.FreeSpaceInfoResponseFromCN freeSpace = ResponsesToClient.FreeSpaceInfoResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received response from controller");
            logger.info("Free space on disk {}",freeSpace.getDiskSpace());
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

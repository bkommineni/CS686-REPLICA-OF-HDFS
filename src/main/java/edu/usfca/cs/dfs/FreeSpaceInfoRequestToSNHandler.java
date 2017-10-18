package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/18/17.
 */
public class FreeSpaceInfoRequestToSNHandler extends StorageNode {
    private RequestsToStorageNode.FreeSpaceInfoRequestToSN freeSpaceInfoRequestToSN;
    private Socket socket;

    public FreeSpaceInfoRequestToSNHandler(RequestsToStorageNode.FreeSpaceInfoRequestToSN freeSpaceInfoRequestToSN) {
        this.freeSpaceInfoRequestToSN = freeSpaceInfoRequestToSN;
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
            ResponsesToClient.FreeSpaceInfoResponseFromSN freeSpaceInfoResponseFromSN = ResponsesToClient.FreeSpaceInfoResponseFromSN.newBuilder()
                    .setNodeCapacity(diskCapacity)
                    .setNodeAvailableSpace((diskCapacity - diskSpaceUsed))
                    .setNodeUsedSpace(diskSpaceUsed).build();
            freeSpaceInfoResponseFromSN.writeDelimitedTo(socket.getOutputStream());
            socket.close();
        }
        catch (IOException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

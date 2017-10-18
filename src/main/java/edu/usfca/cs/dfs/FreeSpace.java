package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bharu on 10/17/17.
 */
public class FreeSpace extends Client {
    public void executeRequest() {
        try {

            Socket socket = new Socket(controllerHostname, controllerPort);
            RequestsToController.ListOfActiveNodesRequest listOfActiveNodesRequest = RequestsToController.ListOfActiveNodesRequest
                    .newBuilder().build();
            RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                    .newBuilder()
                    .setListOfActiveNodesRequestMsg(listOfActiveNodesRequest)
                    .build();
            logger.info("Sending  a list request to Controller...");
            wrapper.writeDelimitedTo(socket.getOutputStream());
            ResponsesToClient.ListOfActiveStorageNodesFromCN response = ResponsesToClient.ListOfActiveStorageNodesFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received response from controller");
            socket.close();

            long totalAvailableSpaceOnCluster = 0;
            long totalCapacityOfCluster = 0;
            long totalUsedSpaceOnCluster = 0;

            for(ResponsesToClient.ListOfActiveStorageNodesFromCN.storageNode SN : response.getSNList())
            {
                logger.info("sending a request to host {} on port {}",SN.getHostname(),SN.getPort());
                socket = new Socket(SN.getHostname(), SN.getPort());
                RequestsToStorageNode.FreeSpaceInfoRequestToSN requestToSN = RequestsToStorageNode.FreeSpaceInfoRequestToSN.newBuilder()
                                                                                .build();
                RequestsToStorageNode.RequestsToStorageNodeWrapper wrapper1 = RequestsToStorageNode.RequestsToStorageNodeWrapper
                                                                            .newBuilder()
                                                                            .setFreeSpaceInfoRequestToSNMsg(requestToSN).build();
                wrapper1.writeDelimitedTo(socket.getOutputStream());
                ResponsesToClient.FreeSpaceInfoResponseFromSN responseFromSN = ResponsesToClient.FreeSpaceInfoResponseFromSN
                                                                                .parseDelimitedFrom(socket.getInputStream());
                totalAvailableSpaceOnCluster = totalAvailableSpaceOnCluster + responseFromSN.getNodeAvailableSpace();
                totalCapacityOfCluster = totalCapacityOfCluster + responseFromSN.getNodeCapacity();
                totalUsedSpaceOnCluster = totalUsedSpaceOnCluster + responseFromSN.getNodeUsedSpace();
                socket.close();
            }
            logger.info("Received response from all nodes");
            logger.info("Cluster Capacity {}", totalCapacityOfCluster);
            logger.info("Cluster Used Space {}", totalUsedSpaceOnCluster);
            logger.info("Cluster Free Space {}", totalAvailableSpaceOnCluster);
            socket.close();
        } catch (IOException e) {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

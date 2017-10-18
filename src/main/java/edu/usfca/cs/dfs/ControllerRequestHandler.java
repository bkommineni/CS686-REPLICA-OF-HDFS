package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by bharu on 10/17/17.
 */
public class ControllerRequestHandler extends Controller implements Runnable {

    private Socket connectionSocket = null;

    public ControllerRequestHandler(Socket connectionSocket)
    {
        this.connectionSocket = connectionSocket;
    }

    @Override
    public void run()
    {
        try
        {
            RequestsToController.RequestsToControllerWrapper msgWrapper = RequestsToController.RequestsToControllerWrapper
                    .parseDelimitedFrom(connectionSocket.getInputStream());

            if(msgWrapper.hasEnrollMsg())
            {
                //enroll storage node
                RequestsToController.Enroll enroll = msgWrapper.getEnrollMsg();
                EnrollRequestToCNHandler handler = new EnrollRequestToCNHandler(enroll);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }

            if(msgWrapper.hasRetrieveFileRequestMsg())
            {
                //retrieve file functionality
                RequestsToController.RetrieveFileRequest retrieve = msgWrapper.getRetrieveFileRequestMsg();
                RetrieveFileRequestToCNHandler handler = new RetrieveFileRequestToCNHandler(retrieve);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }

            if(msgWrapper.hasStoreChunkRequestMsg())
            {
                //store file functionality
                //allocate storage nodes for store file request
                RequestsToController.StoreChunkRequest store = msgWrapper.getStoreChunkRequestMsg();
                StoreChunkRequestToCNHandler handler = new StoreChunkRequestToCNHandler(store);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }

            if(msgWrapper.hasHeartbeatMsg())
            {
                //check info sent on heartbeat and make sure what are active nodes
                RequestsToController.Heartbeat heartbeat = msgWrapper.getHeartbeatMsg();
                HeartBeatRequestToCNHandler handler = new HeartBeatRequestToCNHandler(heartbeat);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }
            if(msgWrapper.hasListOfActiveNodes())
            {
                RequestsToController.ListOfActiveNodesRequest list = msgWrapper.getListOfActiveNodes();
                ListRequestToCNHandler handler = new ListRequestToCNHandler(list);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }
            if(msgWrapper.hasSendGoodChunkRequestMsg())
            {
                RequestsToController.SendGoodChunkRequest goodChunkRequest = msgWrapper.getSendGoodChunkRequestMsg();
                SendGoodChunkRequestToCNHandler handler = new SendGoodChunkRequestToCNHandler(goodChunkRequest);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }
            if(msgWrapper.hasFreeSpaceInfoRequestMsg())
            {
                RequestsToController.FreeSpaceInfoRequest freeSpaceInfoRequest = msgWrapper.getFreeSpaceInfoRequestMsg();
                FreeSpaceInfoRequestHandler handler = new FreeSpaceInfoRequestHandler(freeSpaceInfoRequest);
                handler.setSocket(connectionSocket);
                handler.executeRequest();
            }

        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

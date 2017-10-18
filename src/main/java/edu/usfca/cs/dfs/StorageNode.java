package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageNode {

    protected static final Logger logger = LoggerFactory.getLogger(StorageNode.class);
    protected static final int NUM_THREADS_ALLOWED = 15;
    protected static int controllerPort;
    protected static String controllerPortHostName;
    protected static int storageNodePort;
    protected static Map<String, StorageNodeMetadata> storageNodeMetadataMap = new HashMap<>();
    protected static Map<String, StorageNodeMetadata> dataStoredInLastFiveSeconds = new HashMap<>();
    protected static Socket controllerSocket = null;
    protected static String dataDirectory;
    protected static long diskSpaceUsed;
    protected static long diskCapacity;
    protected ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);

    public static void main(String[] args)
            throws Exception {
        new StorageNode().start(args);
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    protected static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    protected static String calculateChecksum(String filePath) {
        StringBuilder checksum = new StringBuilder();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(filePath)));

            for (int j = 0; j < mdbytes.length; ++j) {
                checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
            }
        } catch (NoSuchAlgorithmException | IOException e) {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
        return checksum.toString();
    }

    private void start(String[] args) throws Exception {
        if (args.length == 4) {
            if (args[0] != null) {
                controllerPortHostName = args[0];
            }
            System.out.println(controllerPortHostName);
            if (args[1] != null) {
                controllerPort = Integer.parseInt(args[1]);
                System.out.println(controllerPort);
            }
            if (args[2] != null) {
                storageNodePort = Integer.parseInt(args[2]);
                System.out.println(controllerPortHostName);
            }
            if (args[3] != null) {
                dataDirectory = args[3];
            }

        }

        File file = new File(dataDirectory);
        diskCapacity = file.getTotalSpace();
        diskSpaceUsed = (file.getTotalSpace() - file.getFreeSpace());
        logger.info("At Start :- diskCapacity {} diskSpaceUsed {}", diskCapacity, diskSpaceUsed);

        logger.info("Enrolling with Controller {} on port {} after entering to network ", controllerPortHostName, controllerPort);
        controllerSocket = new Socket(controllerPortHostName, controllerPort);
        RequestsToController.Enroll enroll = RequestsToController.Enroll.newBuilder()
                .setPort(storageNodePort)
                .setHostname(getHostname())
                .build();
        RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper
                .newBuilder()
                .setEnrollMsg(enroll).build();

        wrapper.writeDelimitedTo(controllerSocket.getOutputStream());
        logger.info("Waiting for Controller to acknowledge enrollment to start server...");
        ResponsesToStorageNode.AcknowledgeEnrollment response = ResponsesToStorageNode.AcknowledgeEnrollment
                .parseDelimitedFrom(controllerSocket.getInputStream());
        logger.info("Successfully enrolled with Controller!!");
        controllerSocket.close();
        logger.info("Starting Storage Node on port {} with data directory on {}", storageNodePort, dataDirectory);
        if (response.getSuccess()) {
            ServerSocket serverSocket = new ServerSocket(storageNodePort);
            logger.info("Listening...");
            executorService.submit(new Thread(new HeartBeat()));
            while (true) {
                Socket connSocket = serverSocket.accept();
                executorService.submit(new Thread(new StorageNodeRequestHandler(connSocket)));
            }
        }
    }

    public class HeartBeat implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    logger.debug("heart beat");
                    controllerSocket = new Socket(controllerPortHostName, controllerPort);
                    List<RequestsToController.Heartbeat.ChunkMetadata> chunkMetadataList = new ArrayList<>();
                    for (String key : dataStoredInLastFiveSeconds.keySet()) {
                        StorageNodeMetadata metadata = dataStoredInLastFiveSeconds.get(key);
                        RequestsToController.Heartbeat.ChunkMetadata chunkMetadata = RequestsToController.Heartbeat.ChunkMetadata.newBuilder()
                                .setChunkId(metadata.getChunkId())
                                .setFilename(metadata.getFilename()).build();
                        chunkMetadataList.add(chunkMetadata);
                    }
                    RequestsToController.Heartbeat.storageNode storageNode = RequestsToController.Heartbeat.storageNode.newBuilder()
                            .setPort(storageNodePort)
                            .setHostname(getHostname())
                            .setDiskSpaceUsed(diskSpaceUsed)
                            .setDiskCapacity(diskCapacity).build();
                    //logger.debug("diskSpaceUsed {} diskCapacity {}",diskSpaceUsed,diskCapacity);

                    RequestsToController.Heartbeat heartbeat = RequestsToController.Heartbeat.newBuilder()
                            .addAllMetadata(chunkMetadataList)
                            .setSN(storageNode)
                            .build();
                    RequestsToController.RequestsToControllerWrapper wrapper = RequestsToController.RequestsToControllerWrapper.newBuilder()
                            .setHeartbeatMsg(heartbeat).build();

                    wrapper.writeDelimitedTo(controllerSocket.getOutputStream());
                    controllerSocket.close();
                    Thread.sleep(5000);
                } catch (InterruptedException | IOException e) {
                    logger.error("exception caught {}", ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

}

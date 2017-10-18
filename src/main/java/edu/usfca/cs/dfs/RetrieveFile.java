package edu.usfca.cs.dfs;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by bharu on 10/17/17.
 */
public class RetrieveFile extends Client
{
    private String filePath;

    public RetrieveFile(String filePath)
    {
        this.filePath = filePath;
    }

    public void executeRequest()
    {
        try
        {
            String currPath = ".";
            Path p = Paths.get(currPath);
            Path absDir = p.toAbsolutePath();
            listOfChunks = new TreeMap<>();
            String fileRequired = filePath;
            String[] tokens = fileRequired.split("/");
            int length = tokens.length;
            String filename = tokens[length - 1];

            String mergedFile = absDir.toString() + "/retrievedFilesDirectory/" + filename;

            RequestsToController.RetrieveFileRequest retrieveFileRequest = RequestsToController.RetrieveFileRequest.newBuilder()
                    .setFilename(filename)
                    .build();
            RequestsToController.RequestsToControllerWrapper requestsToControllerWrapper1 = RequestsToController.RequestsToControllerWrapper.newBuilder().setRetrieveFileRequestMsg(retrieveFileRequest).build();
            logger.info("Sending RetrieveFile request to Controller {} to port {}", controllerHostname, controllerPort);
            Socket socket = new Socket(controllerHostname, controllerPort);
            requestsToControllerWrapper1.writeDelimitedTo(socket.getOutputStream());

            //Response from Controller with Storage Nodes list which host the replicas of chunks of given file
            logger.info("Waiting for RetrieveFile response from Controller...");
            ResponsesToClient.RetrieveFileResponseFromCN responseFromCN = ResponsesToClient.RetrieveFileResponseFromCN.parseDelimitedFrom(socket.getInputStream());
            logger.info("Received RetrieveFile response from Controller...");
            socket.close();

            for (ResponsesToClient.RetrieveFileResponseFromCN.chunkMetadata chunkMetadata : responseFromCN.getChunkListList())
            {
                Thread thread = new Thread(new ChunkRetrieveWorker(chunkMetadata));
                executorService.submit(thread);
            }
            executorService.shutdown();
            executorService.awaitTermination(40, TimeUnit.SECONDS);
            logger.info("byte array size {}", listOfChunks.size());
            Files.deleteIfExists(Paths.get(mergedFile));
            Files.createFile(Paths.get(mergedFile));
            for (int key : listOfChunks.keySet())
            {
                byte[] temp = listOfChunks.get(key);
                Files.write(Paths.get(mergedFile), temp, StandardOpenOption.APPEND);
            }
            logger.info("checksum of retrieved file {}", calculateChecksum(Files.readAllBytes(Paths.get(mergedFile))));
        }
        catch (IOException e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
        catch (InterruptedException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
    }
}

package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    
    protected static final Logger logger = LoggerFactory.getLogger(Client.class);
    protected static SortedMap<Integer,byte[]> listOfChunks;
    private static final int CHUNK_SIZE = 1000000;
    public  static final int NUM_THREADS_ALLOWED = 15;
    protected static ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS_ALLOWED);
    protected static String controllerHostname;
    protected static int controllerPort;
    protected static String typeOfRequest;

    public static void main(String[] args) throws Exception
    {
        controllerHostname = args[0];
        controllerPort = Integer.parseInt(args[1]);
        typeOfRequest = args[2];

        switch (typeOfRequest)
        {
            case "store":
                String filePath = args[3];
                StoreFile storeFile = new StoreFile(filePath);
                storeFile.executeRequest();
                break;
            case "retrieve":
                filePath = args[3];
                RetrieveFile retrieveFile = new RetrieveFile(filePath);
                retrieveFile.executeRequest();
                break;
            case "list":
                ListFiles listFiles = new ListFiles();
                listFiles.executeRequest();
                break;
            case "freeSpace":
                FreeSpace freeSpace = new FreeSpace();
                freeSpace.executeRequest();
                break;
        }
    }

    protected static String calculateChecksum(byte[] bytes)
    {
        StringBuilder checksum = new StringBuilder();
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] mdbytes = md.digest(bytes);
            for (int j = 0; j < mdbytes.length; ++j) {
                checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
            }

        }
        catch (NoSuchAlgorithmException e)
        {
            logger.error("Exception caught {}", ExceptionUtils.getStackTrace(e));
        }
        return checksum.toString();
    }

    protected static List chunking(String filePath)
    {
        List<byte[]> blocks = new ArrayList<>();
        try
        {
            int i = 0;

            byte[] bFile = Files.readAllBytes(new File(filePath).toPath());
            int fileSize = bFile.length;
            logger.info("fileSize in bytes {}", fileSize);

            int numBlocks = (fileSize / CHUNK_SIZE);
            if ((fileSize % CHUNK_SIZE) != 0)
                numBlocks = numBlocks + 1;
            logger.info("number of blocks {}", numBlocks);
            ByteArrayOutputStream bos = null;
            while (i < fileSize) {
                bos = new ByteArrayOutputStream();
                for (int j = 0; j < CHUNK_SIZE; j++) {
                    if (i < fileSize) {
                        bos.write(bFile[i]);
                        i++;
                    }
                }
                blocks.add(bos.toByteArray());
            }
        }
        catch (Exception e)
        {
            logger.error("Exception caught : {}", ExceptionUtils.getStackTrace(e));
        }
        return blocks;
    }

}

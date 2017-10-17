package edu.usfca.cs.dfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 10/13/17.
 */
public class Test {
    private static final int CHUNK_SIZE = 1000000;

    public static void main(String[] args) throws Exception{
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String filepath = absDir.toString() +"/clientDirectory/test_file_1.bin";
        //File file = new File(filepath);
        MessageDigest md = MessageDigest.getInstance("MD5");

        byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(filepath)));
        StringBuilder checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

        String newFilePath = absDir.toString() + "/clientDirectory/test_file_1_1.bin";

        //File file = new File(newFilePath);
        //FileWriter writer = new FileWriter(file);

        byte[] bytes = Files.readAllBytes(Paths.get(filepath));

        /*for(int i=0;i<bytes.length;i++)
        {
            writer.write(bytes[i]);
        }
        writer.close();*/
        Files.write(Paths.get(newFilePath),bytes);

        mdbytes = md.digest(Files.readAllBytes(Paths.get(newFilePath)));
        checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

        List<byte[]> blocks = chunking(filepath);
        String twoFilePath = absDir.toString() + "/clientDirectory/test_file_1_2.bin";
        Files.createFile(Paths.get(twoFilePath));
        for (byte[] block : blocks)
        {
            Files.write(Paths.get(twoFilePath), block,StandardOpenOption.APPEND);
            mdbytes = block;
            checksum = new StringBuilder();
            for (int j = 0; j < mdbytes.length; ++j)
            {
                checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
            }
            System.out.println("checksum for block"+checksum.toString());
        }

        mdbytes = md.digest(Files.readAllBytes(Paths.get(twoFilePath)));
        checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

    }

    private static List chunking(String filePath) throws Exception
    {
        int i=0;

        byte[] bFile = Files.readAllBytes(new File(filePath).toPath());
        int fileSize = bFile.length;

        int numBlocks  = (fileSize / CHUNK_SIZE) ;
        if((fileSize % CHUNK_SIZE) != 0)
            numBlocks = numBlocks + 1;
        List<byte[]> blocks = new ArrayList<>();
        ByteArrayOutputStream bos = null;

        while(i < fileSize)
        {
            bos = new ByteArrayOutputStream();
            for (int j = 0; j < CHUNK_SIZE; j++)
            {
                if(i<fileSize)
                {
                    bos.write(bFile[i]);
                    i++;
                }
            }
            blocks.add(bos.toByteArray());
        }

        return blocks;
    }
}
package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;

/**
 * Created by bharu on 10/13/17.
 */
public class Test {

    public static void main(String[] args) throws Exception{
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String filepath = absDir.toString() +"/clientDirectory/test3";
        //File file = new File(filepath);
        MessageDigest md = MessageDigest.getInstance("MD5");

        byte[] mdbytes = md.digest(Files.readAllBytes(Paths.get(filepath)));
        StringBuilder checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

        String newFilePath = absDir.toString() + "/clientDirectory/test4";

        File file = new File(newFilePath);
        FileWriter writer = new FileWriter(file);

        byte[] bytes = Files.readAllBytes(Paths.get(filepath));

        for(int i=0;i<bytes.length;i++)
        {
            writer.write(bytes[i]);
        }
        writer.close();

        mdbytes = md.digest(Files.readAllBytes(Paths.get(newFilePath)));
        checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

        String twoFilePath = absDir.toString() + "/clientDirectory/test5";

        mdbytes = md.digest(Files.readAllBytes(Paths.get(twoFilePath)));
        checksum = new StringBuilder();
        for (int j = 0; j < mdbytes.length; ++j)
        {
            checksum.append(Integer.toHexString((mdbytes[j] & 0xFF) | 0x100).substring(1, 3));
        }
        System.out.println("checksum {}"+checksum.toString());

    }
}

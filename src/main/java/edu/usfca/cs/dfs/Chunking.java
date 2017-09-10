package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by bharu on 9/10/17.
 */
public class Chunking {

    public static void main(String[] args) throws Exception{
        String currPath = ".";
        Path p = Paths.get(currPath);
        Path absDir = p.toAbsolutePath();
        String filePath = absDir.toString() + "/data/File1.txt";

        byte[] bFile = Files.readAllBytes(new File(filePath).toPath());

        System.out.println(bFile.length);

        String filePath1 = absDir.toString()+"/data/File1Part";

        int i=0;
        int j=1;
        int blocksize = 2000;
        int filePart = 1;
        FileWriter writer = new FileWriter(filePath1.concat(Integer.toString(filePart)).concat(".txt"));

        while (i < bFile.length )
        {

            if(j==blocksize)
            {
                filePart = filePart + 1;
                writer.close();
                writer = new FileWriter(filePath1.concat(Integer.toString(filePart)).concat(".txt"));
                j=1;
            }
            writer.write(bFile[i]);
            i++;
            j++;
        }
        writer.close();
    }
}

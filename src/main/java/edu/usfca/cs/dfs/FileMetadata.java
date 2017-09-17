package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 9/10/17.
 */
public class FileMetadata {

    private String fileName;
    private List<ChunkMetadata> chunkMetadatas = new ArrayList<>();

    public FileMetadata(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void addChunkMetadata(ChunkMetadata metadata) {
        chunkMetadatas.add(metadata);
    }

    public void setChunkMetadatas(List<ChunkMetadata> chunkMetadatas) {
        this.chunkMetadatas = chunkMetadatas;
    }

    public List<ChunkMetadata> getChunkMetadatas() {
        return chunkMetadatas;
    }

    @Override
    public boolean equals(Object obj) {
        if(this.fileName.equals(obj))
            return true;
        else
            return false;
    }
}

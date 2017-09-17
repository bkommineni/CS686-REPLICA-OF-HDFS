package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 9/10/17.
 */
public class ChunkMetadata {

    private String filename;
    private int chunkId;
    private List<ReplicaMetadata> replicaMetadatas = new ArrayList<>();

    public ChunkMetadata(String filename, int chunkId) {
        this.filename = filename;
        this.chunkId = chunkId;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public int getChunkId() {
        return chunkId;
    }

    public void setChunkId(int chunkId) {
        this.chunkId = chunkId;
    }

    public void addReplicaMetadata(ReplicaMetadata metadata) {
        replicaMetadatas.add(metadata);
    }

    public void setReplicaMetadatas(List<ReplicaMetadata> replicaMetadatas) {
        this.replicaMetadatas = replicaMetadatas;
    }
}

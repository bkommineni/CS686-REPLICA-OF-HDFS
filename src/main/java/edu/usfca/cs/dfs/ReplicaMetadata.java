package edu.usfca.cs.dfs;

/**
 * Created by bharu on 9/10/17.
 */
public class ReplicaMetadata {

    private String filename;
    private int blockId;
    private DataNode dataNode;

    public ReplicaMetadata(String filename, int blockId, DataNode dataNode) {
        this.filename = filename;
        this.blockId = blockId;
        this.dataNode = dataNode;
    }
}

package edu.usfca.cs.dfs;

/**
 * Created by bharu on 9/16/17.
 */
public class Metadata {
    private String filename;
    private int chunkId;
    private DataNode dataNode;

    public Metadata(String filename, int chunkId) {
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

    public DataNode getDataNode() {
        return dataNode;
    }

    public void setDataNode(DataNode dataNode) {
        this.dataNode = dataNode;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "filename='" + filename + '\'' +
                ", chunkId=" + chunkId +
                ", dataNode=" + dataNode +
                '}';
    }
}

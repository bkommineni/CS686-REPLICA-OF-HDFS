package edu.usfca.cs.dfs;

/**
 * Created by bharu on 9/19/17.
 */
public class StorageNodeMetadata {

    private String filename;
    private int chunkId;
    private byte[] checksum;

    public StorageNodeMetadata(String filename, int chunkId) {
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

    public byte[] getChecksum() {
        return checksum;
    }

    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }
}

package edu.usfca.cs.dfs;

/**
 * Created by bharu on 9/10/17.
 */
public class DataNode {

    private int port;
    private String hostname;
    private long  diskspaceUsed;
    private long  diskCapacity;

    public DataNode(int port,String hostname) {
        this.port = port;
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getDiskspaceUsed() {
        return diskspaceUsed;
    }

    public void setDiskspaceUsed(long diskspaceUsed) {
        this.diskspaceUsed = diskspaceUsed;
    }

    public long getDiskCapacity() {
        return diskCapacity;
    }

    public void setDiskCapacity(long diskCapacity) {
        this.diskCapacity = diskCapacity;
    }

    @Override
    public String toString() {
        return "DataNode{" +
                "port=" + port +
                ", hostname='" + hostname + '\'' +
                '}';
    }
}

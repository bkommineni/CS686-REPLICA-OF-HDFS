package edu.usfca.cs.dfs;

/**
 * Created by bharu on 9/10/17.
 */
public class DataNode {

    private int port;
    private String hostname;

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
}

package cloud.domain;

public class ClusterInfo {
    private String id;
    private String host;
    private int port;
    private volatile long lastHeartbeat;

    public ClusterInfo() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public ClusterInfo(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public void heartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
}

package cloud.CloudManager;

public class ClusterInfo {
    private final String id;
    private final String host;
    private final int port;
    private volatile long lastHeartbeat;

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
    public String getHost() { return host; }
    public int getPort() { return port; }
    public long getLastHeartbeat() { return lastHeartbeat; }
}

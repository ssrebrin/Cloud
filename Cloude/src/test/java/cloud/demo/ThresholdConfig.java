package cloud.demo;

import java.io.Serializable;

public class ThresholdConfig implements Serializable {
    private final int workThreshold;
    private final int playThreshold;

    public ThresholdConfig(int workThreshold, int playThreshold) {
        this.workThreshold = workThreshold;
        this.playThreshold = playThreshold;
    }

    public int getWorkThreshold() { return workThreshold; }
    public int getPlayThreshold() { return playThreshold; }
}

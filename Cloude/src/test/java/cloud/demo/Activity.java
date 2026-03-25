package cloud.demo;

import java.io.Serializable;

public class Activity implements Serializable {
    private final String type;
    private final int durationMinutes;

    public Activity(String type, int durationMinutes) {
        this.type = type;
        this.durationMinutes = durationMinutes;
    }

    public String getType() { return type; }
    public int getDurationMinutes() { return durationMinutes; }
}

package cloud.demo;

import java.io.Serializable;
import java.util.List;

public class Profile implements Serializable {
    private final String username;
    private final List<Activity> activities;

    public Profile(String username, List<Activity> activities) {
        this.username = username;
        this.activities = activities;
    }

    public String getUsername() { return username; }
    public List<Activity> getActivities() { return activities; }
}

package cloud.demo;

public class ActivityAnalyzer {
    public static int calculateScore(Profile profile, ThresholdConfig config) {
        int score = 0;
        for (Activity activity : profile.getActivities()) {
            if ("WORK".equals(activity.getType())) {
                score += activity.getDurationMinutes() > config.getWorkThreshold() ? 10 : 5;
            } else if ("PLAY".equals(activity.getType())) {
                score -= activity.getDurationMinutes() > config.getPlayThreshold() ? 5 : 2;
            }
        }
        return score;
    }
}

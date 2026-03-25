package cloud.demo;

import cloud.CloudManager.TaskScheduler;
import cloud.Cluster.Worker;
import cloud.cloud.Cloud;
import cloud.cloud.RemoteFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloudComplexTest {

    private static TaskScheduler scheduler;
    private static Worker worker;
    private static final int MANAGER_PORT = 8089;
    private static final int WORKER_PORT = 8090;

    @BeforeAll
    public static void setup() throws Exception {
        // Запуск Менеджера на тестовом порту
        scheduler = new TaskScheduler(MANAGER_PORT);
        
        // Запуск Воркера, подключенного к тестовому менеджеру
        worker = new Worker("localhost", MANAGER_PORT, "localhost", WORKER_PORT, 2);
        worker.startServer();
        worker.startResultSender();
        
        Thread.sleep(1000);
    }

    @AfterAll
    public static void tearDown() {
        if (worker != null) worker.stop();
        if (scheduler != null) scheduler.shutdown();
    }

    @Test
    public void testComplexAnalytics() throws Exception {
        List<Profile> profiles = Arrays.asList(
            new Profile("user1", Arrays.asList(new Activity("WORK", 120), new Activity("PLAY", 30))),
            new Profile("user2", Arrays.asList(new Activity("WORK", 30), new Activity("PLAY", 120)))
        );

        ThresholdConfig config = new ThresholdConfig(60, 60);

        // Лямбда: использует захваченный config и статический анализатор
        RemoteFunction<Integer, Integer> analyzeTask = (Integer index) -> {
            Profile p = profiles.get(index);
            return ActivityAnalyzer.calculateScore(p, config);
        };

        Cloud cloud = Cloud.connect("http://localhost:" + MANAGER_PORT);

        List<Integer> scores = cloud.execute(analyzeTask, new int[]{0, 1}, 
                Profile.class, Activity.class, ThresholdConfig.class, ActivityAnalyzer.class);

        System.out.println("Productivity scores: " + scores);

        // user1: WORK(120>60 -> 10) + PLAY(30<60 -> -2) = 8
        // user2: WORK(30<60 -> 5) + PLAY(120>60 -> -5) = 0
        assertTrue(scores.contains(8));
        assertTrue(scores.contains(0));
    }
}

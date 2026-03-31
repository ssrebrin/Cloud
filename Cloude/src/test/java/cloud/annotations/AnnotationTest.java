package cloud.annotations;

import cloud.CloudManager.TaskScheduler;
import cloud.Cluster.Worker;
import cloud.cloud.Cloud;
import cloud.domain.RemoteFunction;
import cloud.serialization.CloudClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnnotationTest {

    private static TaskScheduler scheduler;
    private static Worker worker;
    private static final int MANAGER_PORT = 8091;
    private static final int WORKER_PORT = 8092;

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

    /**
     * Вспомогательный класс, помеченный аннотацией @CloudClass.
     * Он должен автоматически попасть в JAR и быть доступным на воркере.
     */
    @CloudClass
    public static class AnnotatedHelper {
        public static int multiply(int x, int y) {
            return x * y;
        }
    }

    @Test
    public void testAnnotationBasedExecution() throws Exception {
        Cloud cloud = Cloud.connect("http://localhost:" + MANAGER_PORT);

        RemoteFunction<Integer, Integer> fn = x -> AnnotatedHelper.multiply(x, 10);

        List<Integer> result = cloud.execute(fn, new int[]{1, 2, 3});

        System.out.println("Annotation test result: " + result);

        assertEquals(3, result.size());
        assertTrue(result.contains(10));
        assertTrue(result.contains(20));
        assertTrue(result.contains(30));
    }
}

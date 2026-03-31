package cloud.demo;

import cloud.CloudManager.TaskScheduler;
import cloud.Cluster.Worker;
import cloud.cloud.Cloud;
import cloud.domain.RemoteFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Тест для проверки распределения задач между несколькими воркерами.
 * Запускает 1 менеджер и 3 воркера.
 */
public class CloudMultiWorkerTest {

    private static TaskScheduler scheduler;
    private static List<Worker> workers = new ArrayList<>();
    private static final int MANAGER_PORT = 8091;
    private static final int BASE_WORKER_PORT = 8092;

    public static void startWorker(int index) throws Exception {
        int workerPort = BASE_WORKER_PORT + index;
        // Используем 1 поток на воркере, чтобы видеть распределение задач менеджером
        Worker worker = new Worker("localhost", MANAGER_PORT, "localhost", workerPort, 1);
        worker.startServer();
        worker.startResultSender();
        workers.add(worker);
    }

    @BeforeAll
    public static void setup() throws Exception {
        scheduler = new TaskScheduler(MANAGER_PORT);
    }

    @AfterEach
    public void cleanup() {
        for (Worker worker : workers) {
            worker.stop();
        }
        workers.clear();
    }

    @AfterAll
    public static void tearDown() {
        for (Worker worker : workers) {
            worker.stop();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    public void testParallelEfficiency() throws Exception {
        // Задача с задержкой 200 мс
        RemoteFunction<Integer, Integer> heavyTask = (Integer x) -> {
            try { Thread.sleep(200); } catch (InterruptedException e) {}
            return x * 2;
        };

        // Обрабатываем по 10 элементов
        int[] input = IntStream.range(0, 10).toArray();
        Cloud cloud = Cloud.connect("http://localhost:" + MANAGER_PORT);

        // 1 воркер
        startWorker(0);
        Thread.sleep(1000);

        long start1 = System.currentTimeMillis();
        cloud.execute(heavyTask, input);
        long end1 = System.currentTimeMillis();
        long time1 = end1 - start1;
        System.out.println("Time with 1 worker: " + time1 + " ms");

        // 3 воркера
        startWorker(1);
        startWorker(2);
        Thread.sleep(1000);

        // Теперь подаем 30 элементов (3 батча по 10)
        int[] input30 = IntStream.range(0, 30).toArray();

        long start3 = System.currentTimeMillis();
        cloud.execute(heavyTask, input30);
        long end3 = System.currentTimeMillis();
        long time3 = end3 - start3;
        System.out.println("Time with 3 workers (triple load): " + time3 + " ms");
        
        assertTrue(time3 < time1 * 2, "3 workers should be significantly faster than processing 3 batches sequentially");
    }

    @Test
    public void testTripleWorkerDistribution() throws Exception {
        if (workers.isEmpty()) {
            startWorker(0);
            Thread.sleep(1000);
        }
        
        List<Integer> input = IntStream.range(0, 30).boxed().toList();
        int[] inputArr = input.stream().mapToInt(i -> i).toArray();

        RemoteFunction<Integer, Integer> multiplyTask = (Integer x) -> x * 2;
        Cloud cloud = Cloud.connect("http://localhost:" + MANAGER_PORT);

        List<Integer> results = cloud.execute(multiplyTask, inputArr);
        assertEquals(30, results.size());
    }
}

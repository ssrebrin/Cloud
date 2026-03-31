package cloud.cloud;

import cloud.CloudManager.TaskScheduler;
import cloud.Cluster.Worker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CloudStreamIntegrationTest {

    private static TaskScheduler scheduler;
    private static final List<Worker> workers = new ArrayList<>();
    private static String managerUrl;
    private static int managerPort;

    @BeforeAll
    static void setup() throws Exception {
        managerPort = findFreePort();
        managerUrl = "http://localhost:" + managerPort;

        scheduler = new TaskScheduler(managerPort);
        Thread.sleep(400);

        startWorker(findFreePort());
        startWorker(findFreePort());
        startWorker(findFreePort());
        Thread.sleep(800);
    }

    @AfterAll
    static void tearDown() {
        for (Worker worker : workers) {
            worker.stop();
        }
        workers.clear();

        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void mapOnly() throws Exception {
        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(new int[]{1, 2, 3, 4})
                .map(x -> x * 3)
                .execute();

        assertEquals(List.of(3, 6, 9, 12), result);
    }

    @Test
    void filterOnly() throws Exception {
        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(new int[]{1, 2, 3, 4, 5, 6})
                .filter(x -> x % 2 == 0)
                .execute();

        assertEquals(List.of(2, 4, 6), result);
    }

    @Test
    void reduceOnly() throws Exception {
        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(new int[]{1, 2, 3, 4})
                .reduce(Integer::sum)
                .execute();

        assertEquals(List.of(10), result);
    }

    @Test
    void mapFilterReduce() throws Exception {
        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(new int[]{1, 2, 3, 4, 5, 6, 7, 8})
                .map(x -> x * 2)
                .filter(x -> x % 4 == 0)
                .reduce(Integer::sum)
                .execute();

        assertEquals(List.of(40), result);
    }

    @Test
    void reduceMultiRoundLargeInput() throws Exception {
        int[] input = IntStream.rangeClosed(1, 200).toArray();

        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(input)
                .reduce(Integer::sum)
                .execute();

        assertEquals(List.of(20100), result);
    }

    @Test
    void reduceEmptyInput() throws Exception {
        List<Integer> result = executeWithRetry(3, () ->
                CloudStream.<Integer>connect(managerUrl)
                        .stream(new int[]{})
                        .reduce(Integer::sum)
                        .execute()
        );

        assertEquals(List.of(), result);
    }

    @Test
    void reduceSingleElement() throws Exception {
        List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                .stream(new int[]{42})
                .reduce(Integer::sum)
                .execute();

        assertEquals(List.of(42), result);
    }

    private static void startWorker(int workerPort) throws Exception {
        Worker worker = new Worker("localhost", managerPort, "localhost", workerPort, 2);
        worker.startServer();
        worker.startResultSender();
        workers.add(worker);
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static <R> R executeWithRetry(int attempts, ThrowingSupplier<R> action) throws Exception {
        IOException lastException = null;
        for (int i = 0; i < attempts; i++) {
            try {
                return action.get();
            } catch (IOException e) {
                lastException = e;
                Thread.sleep(150);
            }
        }
        throw lastException;
    }

    @FunctionalInterface
    private interface ThrowingSupplier<R> {
        R get() throws Exception;
    }
}

package cloud.cloud;

import cloud.CloudManager.TaskScheduler;
import cloud.Cluster.Worker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClojureCloudIntegrationTest {

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
    void executeLegacyClojureFunction() throws Exception {
        List<Integer> result = ClojureCloud.connect(managerUrl)
                .execute("(fn [x] (* x x))", new int[]{1, 2, 3, 4});

        assertEquals(List.of(1, 4, 9, 16), result);
    }

    @Test
    void executeClojurePipeline() throws Exception {
        List<Object> result = ClojureCloudStream.connect(managerUrl)
                .stream(new int[]{1, 2, 3, 4, 5, 6})
                .filter("(fn [x] (zero? (mod x 2)))")
                .map("(fn [x] (* x 10))")
                .reduce("(fn [a b] (+ a b))")
                .execute();

        assertEquals(1, result.size());
        assertEquals(120, ((Number) result.get(0)).intValue());
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
}

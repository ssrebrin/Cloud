package cloud.Cluster;

import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;
import cloud.CloudManager.WorkerTask;
import cloud.serialization.CloudClassLoader;
import cloud.serialization.KryoSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import javax.xml.transform.Result;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker {

    private final String managerHost;
    private final int managerPort;
    private final String hostName;
    private final int port;
    private final String id;
    private int workerThreads;

    private final ObjectMapper mapper = new ObjectMapper();
    private ExecutorService workerPool;

    private final BlockingQueue<WorkerTask> Tasks;
    private final BlockingQueue<TaskResult> Results;

    private final KryoSerializer serializer = new KryoSerializer();

    public TaskResult process(WorkerTask task) {
        try {
            CloudClassLoader classLoader = new CloudClassLoader(this.getClass().getClassLoader());
            
            // Загрузка JAR с кодом (если есть)
            if (task.getJarBytes() != null) {
                byte[] jarBytes = java.util.Base64.getDecoder().decode(task.getJarBytes());
                classLoader.addJar(jarBytes);
            }

            // Десериализация функции
            byte[] fnBytes = java.util.Base64.getDecoder().decode(task.getSerializedFunction());
            cloud.cloud.RemoteFunction<Integer, Integer> fn = 
                (cloud.cloud.RemoteFunction<Integer, Integer>) serializer.deserialize(fnBytes, classLoader);

            // Выполнение
            List<Integer> results = new ArrayList<>();
            for (Integer val : task.getValues()) {
                results.add(fn.apply(val));
            }

            return new TaskResult<>(task.getTaskId(), results, null, task.getWorkerTaskId());

        } catch (Exception e) {
            e.printStackTrace();
            return new TaskResult<>(task.getTaskId(), null, e.getMessage(), task.getWorkerTaskId());
        }
    }

    public void startWorkers(int threads) {
        workerPool = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            workerPool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        WorkerTask task = Tasks.take();
                        TaskResult result = process(task);
                        Results.put(result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public Worker(String managerHost, int managerPort, String hostName, int port, int workerThreads) {
        this.managerHost = managerHost;
        this.managerPort = managerPort;
        this.hostName = hostName;
        this.port = port;
        this.id = UUID.randomUUID().toString();
        this.Tasks = new LinkedBlockingQueue<>();
        this.Results = new LinkedBlockingQueue<>();
        this.workerThreads = workerThreads;
    }

    public boolean register() {
        try {
            URL url = new URL("http://" + managerHost + ":" + managerPort + "/register");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            Map<String, Object> payload = Map.of(
                    "id", id,
                    "host", hostName,
                    "port", port
            );

            String json = mapper.writeValueAsString(payload);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Cluster registered successfully: " + id);
                return true;
            }

            System.out.println("Failed to register cluster. Response code: " + responseCode);
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private HttpServer server;

    public void startServer() throws IOException {
        if (!register()) {
            throw new RuntimeException("Failed to register cluster");
        }

        startWorkers(this.workerThreads);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/execute", new ExecuteHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Cluster server started on port " + port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (workerPool != null) {
            workerPool.shutdownNow();
        }
    }

    private class ExecuteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                WorkerTask task = mapper.readValue(exchange.getRequestBody().readAllBytes(), WorkerTask.class);

                Tasks.add(task);

                writeJson(exchange, 202, Map.of(
                        "status", "accepted",
                        "taskId", task.getTaskId()
                ));

                System.out.println("New task");


            } catch (Exception e) {
                e.printStackTrace();
                writeJson(exchange, 500, Map.of(
                        "status", "error",
                        "error", e.getMessage()
                ));
            }
        }
    }

    private void sendResultToManager(TaskResult result) {
        try {
            URL url = new URL("http://" + managerHost + ":" + managerPort + "/result");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            String json = mapper.writeValueAsString(result);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            if (code != 200) {
                System.out.println("Failed to send result: " + code);
            }
            System.out.println("Task sent successfully: " + result.getWorkerTaskId() + " : " + result.getTaskId());

        } catch (Exception e) {
            e.printStackTrace();
            // 👉 можно добавить retry позже
        }
    }

    public void startResultSender() {
        Thread sender = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TaskResult result = Results.take();
                    sendResultToManager(result);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        sender.setDaemon(true);
        sender.start();
    }

    private void writeJson(HttpExchange exchange, int statusCode, Map<String, Object> payload) throws IOException {
        byte[] json = mapper.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, json.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(json);
        }
    }

    public static void main(String[] args) throws IOException {
        Worker worker = new Worker("localhost", 8085, "localhost", 8086, 5);
        worker.startServer();
        worker.startWorkers(1);
        worker.startResultSender();
    }
}

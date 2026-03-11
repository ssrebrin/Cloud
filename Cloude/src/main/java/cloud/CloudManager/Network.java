package cloud.CloudManager;

import cloud.CloudManager.TaskResult;
import cloud.cloud.RemoteFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Network implements Runnable {
    private static BlockingQueue<cloud.CloudManager.Task> outgoingTasks;
    private final BlockingQueue<TaskResult> incomingResults;
    private final ConcurrentHashMap<String, cloud.CloudManager.ClusterInfo> clusters;
    private final int port;

    public Network(BlockingQueue<cloud.CloudManager.Task> tasks, BlockingQueue<TaskResult> results,
                   ConcurrentHashMap<String, cloud.CloudManager.ClusterInfo> clusters, int port
    ) {
        this.outgoingTasks = tasks;
        this.incomingResults = results;
        this.clusters = clusters;
        this.port = port;
    }

    static class RegisterHandler implements HttpHandler {

        private final ObjectMapper mapper = new ObjectMapper();
        private final Map<String, cloud.CloudManager.ClusterInfo> clusters;

        public RegisterHandler(Map<String, cloud.CloudManager.ClusterInfo> clusters) {
            this.clusters = clusters;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes());

            Map<String, Object> request = mapper.readValue(body, Map.class);

            String id = (String) request.get("id");
            String host = (String) request.get("host");
            int port = ((Number) request.get("port")).intValue();

            cloud.CloudManager.ClusterInfo cluster =
                    new cloud.CloudManager.ClusterInfo(id, host, port);

            clusters.put(id, cluster);

            System.out.println("Cluster registered: " + id);

            Map<String, String> response = Map.of(
                    "status", "accepted",
                    "clusterId", id
            );

            byte[] json = mapper.writeValueAsBytes(response);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, json.length);

            OutputStream os = exchange.getResponseBody();
            os.write(json);
            os.close();
        }
    }

    static class ExecuteHandler implements HttpHandler {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                System.out.println("New task");
                byte[] bodyBytes = exchange.getRequestBody().readAllBytes();

                Task<?, ?> task = mapper.readValue(bodyBytes, Task.class);

                outgoingTasks.put(task);

                Map<String, String> response = Map.of(
                        "status", "accepted",
                        "taskId", task.getId()
                );
                byte[] json = mapper.writeValueAsBytes(response);

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, json.length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(json);
                }

            } catch (Exception e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    @Override
    public void run() {

        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(this.port), 0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        server.createContext("/execute", new ExecuteHandler());
        server.createContext("/register", new RegisterHandler(this.clusters));
        server.setExecutor(null);
        server.start();
        System.out.println("Server started at http://localhost:" + port);
    }
}

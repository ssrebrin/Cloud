package cloud.Cluster;

import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

public class Cluster {

    private final String managerHost;
    private final int managerPort;
    private final String hostName;
    private final int port;
    private final String id;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Worker worker = new Worker();

    public Cluster(String managerHost, int managerPort, String hostName, int port) {
        this.managerHost = managerHost;
        this.managerPort = managerPort;
        this.hostName = hostName;
        this.port = port;
        this.id = UUID.randomUUID().toString();
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

    public void startServer() throws IOException {
        if (!register()) {
            throw new RuntimeException("Failed to register cluster");
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/execute", new ExecuteHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Cluster server started on port " + port);
    }

    private class ExecuteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                Task task = mapper.readValue(exchange.getRequestBody().readAllBytes(), Task.class);
                TaskResult<java.util.List<Integer>> result = worker.execute(task);

                if (result.isSuccess()) {
                    writeJson(exchange, 200, Map.of(
                            "status", "done",
                            "taskId", result.getTaskId(),
                            "result", result.getResult()
                    ));
                } else {
                    writeJson(exchange, 200, Map.of(
                            "status", "error",
                            "taskId", result.getTaskId(),
                            "error", result.getError()
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
                writeJson(exchange, 500, Map.of("status", "error", "error", e.getMessage()));
            }
        }
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
        Cluster cluster = new Cluster("localhost", 8085, "localhost", 8086);
        cluster.startServer();
    }
}

package cloud.CloudManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Network implements Runnable {

    private final BlockingQueue<Task> outgoingTasks;
    private final ConcurrentHashMap<String, ClusterInfo> clusters;
    private final ConcurrentHashMap<String, TaskResult<List<Integer>>> completedResults;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, TaskAggregator> aggregators;

    public Network(
            BlockingQueue<Task> outgoingTasks,
            ConcurrentHashMap<String, ClusterInfo> clusters,
            ConcurrentHashMap<String, TaskResult<List<Integer>>> completedResults,
            ConcurrentHashMap<String, TaskAggregator> aggregators,
            int port
    ) {
        this.outgoingTasks = outgoingTasks;
        this.clusters = clusters;
        this.completedResults = completedResults;
        this.aggregators = aggregators;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/execute", new ExecuteHandler());
            server.createContext("/register", new RegisterHandler());
            server.createContext("/result", new ResultHandler());
            server.setExecutor(null);
            server.start();
            System.out.println("Server started at http://localhost:" + port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class RegisterHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            Map<String, Object> request = mapper.readValue(exchange.getRequestBody().readAllBytes(), Map.class);
            String id = String.valueOf(request.get("id"));
            String host = String.valueOf(request.get("host"));
            int port = ((Number) request.get("port")).intValue();

            clusters.put(id, new ClusterInfo(id, host, port));
            System.out.println("Cluster registered: " + id);

            writeJson(exchange, 200, Map.of("status", "accepted", "clusterId", id));
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
                Map<String, Object> request = mapper.readValue(exchange.getRequestBody().readAllBytes(), Map.class);
                String functionStub = String.valueOf(request.get("functionStub"));
                List<Integer> values = parseIntegerList(request.get("values"));
                String callback = String.valueOf(request.get("callback"));

                Task task = new Task(functionStub, values, callback);
                outgoingTasks.put(task);

                writeJson(exchange, 200, Map.of("status", "accepted", "taskId", task.getId()));
            } catch (Exception e) {
                e.printStackTrace();
                writeJson(exchange, 500, Map.of("status", "error", "error", e.getMessage()));
            }
        }
    }

    private class ResultHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET":
                    String path = exchange.getRequestURI().getPath();
                    String prefix = "/result/";
                    if (!path.startsWith(prefix) || path.length() <= prefix.length()) {
                        writeJson(exchange, 400, Map.of("status", "error", "error", "taskId is missing"));
                        return;
                    }

                    String taskId = path.substring(prefix.length());
                    TaskResult<List<Integer>> result = completedResults.get(taskId);
                    if (result == null) {
                        writeJson(exchange, 202, Map.of("status", "pending"));
                        return;
                    }

                    if (result.isSuccess()) {
                        writeJson(exchange, 200, Map.of("status", "done", "taskId", taskId, "result", result.getResult()));
                    } else {
                        writeJson(exchange, 200, Map.of("status", "error", "taskId", taskId, "error", result.getError()));
                    }

                    System.out.println("Task sent");
                    break;
                case "POST":
                    try {
                        TaskResult<List<Integer>> workerResult =
                                mapper.readValue(exchange.getRequestBody().readAllBytes(),
                                        new TypeReference<TaskResult<List<Integer>>>() {});

                        String taskIdd = workerResult.getTaskId();

                        TaskAggregator aggregator = aggregators.get(taskIdd);
                        System.out.println(">" + aggregators);
                        System.out.println("Worker task sent " + taskIdd);

                        if (aggregator == null) {
                            writeJson(exchange, 404, Map.of(
                                    "status", "error",
                                    "error", "Unknown taskId"
                            ));
                            return;
                        }

                        String workerTaskId = workerResult.getWorkerTaskId();

                        if (workerResult.isSuccess()) {
                            aggregator.complete(workerTaskId, workerResult.getResult());
                        } else {
                            aggregator.fail(workerTaskId, workerResult.getError());
                        }

                        if (aggregator.isFinished()) {
                            TaskResult<List<Integer>> finalResult = aggregator.buildResult();

                            completedResults.put(taskIdd, finalResult);
                            aggregators.remove(taskIdd);
                        }

                        writeJson(exchange, 200, Map.of(
                                "status", "ok"
                        ));

                    } catch (Exception e) {
                        e.printStackTrace();

                        writeJson(exchange, 500, Map.of(
                                "status", "error",
                                "error", e.getMessage()
                        ));
                    }
                    break;
                default:
                        exchange.sendResponseHeaders(405, -1);
                        return;

            }
        }
    }

    private List<Integer> parseIntegerList(Object rawList) {
        if (!(rawList instanceof List<?> list)) {
            return List.of();
        }

        return list.stream()
                .map(value -> ((Number) value).intValue())
                .toList();
    }

    private void writeJson(HttpExchange exchange, int statusCode, Map<String, Object> payload) throws IOException {
        byte[] json = mapper.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, json.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(json);
        }
    }
}

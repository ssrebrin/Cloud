package cloud.CloudManager;

import cloud.domain.ClusterInfo;
import cloud.domain.Operation;
import cloud.domain.Task;
import cloud.domain.TaskResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Network implements Runnable {

    private final BlockingQueue<Task> outgoingTasks;
    private final ConcurrentHashMap<String, ClusterInfo> clusters;
    private final ConcurrentHashMap<String, TaskResult<?>> completedResults;
    private final int port;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, TaskAggregator> aggregators;
    private HttpServer server;

    private final TaskScheduler scheduler;

    public Network(
            BlockingQueue<Task> outgoingTasks,
            ConcurrentHashMap<String, ClusterInfo> clusters,
            ConcurrentHashMap<String, TaskResult<?>> completedResults,
            ConcurrentHashMap<String, TaskAggregator> aggregators,
            int port,
            TaskScheduler scheduler
    ) {
        this.outgoingTasks = outgoingTasks;
        this.clusters = clusters;
        this.completedResults = completedResults;
        this.aggregators = aggregators;
        this.port = port;
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
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

    public void stop() {
        if (server != null) {
            server.stop(0);
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
                
                // Check if this is a pipeline request (has ops)
                if (request.containsKey("ops")) {
                    // Pipeline task
                    List<Operation> ops = mapper.convertValue(request.get("ops"), 
                            new TypeReference<List<Operation>>() {});
                    Object data = request.get("data");
                    String jarBytes = request.containsKey("jarBytes") ? String.valueOf(request.get("jarBytes")) : null;
                    String callback = request.containsKey("callback") ? String.valueOf(request.get("callback")) : null;


                    System.out.println(">>" + data);
                    Task task = new Task(ops, data, jarBytes, callback);
                    outgoingTasks.put(task);
                    
                    writeJson(exchange, 200, Map.of("status", "accepted", "taskId", task.getId()));
                    System.out.println("Pipeline task accepted: " + task.getId() + " with " + ops.size() + " operations");
                } else {
                    // Legacy single function task
                    String functionStub = String.valueOf(request.get("functionStub"));
                    String serializedFunction = String.valueOf(request.get("serializedFunction"));
                    String jarBytes = String.valueOf(request.get("jarBytes"));
                    List<Integer> values = parseIntegerList(request.get("values"));
                    String callback = String.valueOf(request.get("callback"));

                    Task task = new Task(functionStub, serializedFunction, jarBytes, values, callback);
                    outgoingTasks.put(task);

                    writeJson(exchange, 200, Map.of("status", "accepted", "taskId", task.getId()));
                    System.out.println("Legacy task accepted: " + task.getId());
                }
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
                    TaskResult<?> result = completedResults.get(taskId);
                    if (result == null) {
                        writeJson(exchange, 202, Map.of("status", "pending"));
                        return;
                    }

                    if (result.isSuccess()) {
                        Map<String, Object> payload = new HashMap<>();
                        payload.put("status", "done");
                        payload.put("taskId", taskId);
                        payload.put("result", result.getResult());
                        writeJson(exchange, 200, payload);
                    } else {
                        writeJson(exchange, 200, Map.of("status", "error", "taskId", taskId, "error", result.getError()));
                    }

                    System.out.println("Task sent");
                    break;
                case "POST":
                    try {
                        // Use wildcard type to handle any result type (Integer, Boolean, etc.)
                        TaskResult<?> workerResult =
                                mapper.readValue(exchange.getRequestBody().readAllBytes(),
                                        new TypeReference<TaskResult<?>>() {});

                        String taskIdd = workerResult.getTaskId();
                        // Check if this is a pipeline task result
                        Task task = scheduler.getTask(taskIdd);
                        if (task != null && task.isPipeline()) {
                            System.out.println("Pipeline result received for task: " + taskIdd + ", opIndex: " + (task.getCurrentOpIndex()));
                            scheduler.onPipelineResult(task, (TaskResult<Object>) workerResult);
                            writeJson(exchange, 200, Map.of("status", "ok"));
                            return;
                        }

                        // Legacy task handling - cast to List<Integer>
                        TaskAggregator aggregator = aggregators.get(taskIdd);
                        System.out.println(">" + aggregator);
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
                            @SuppressWarnings("unchecked")
                            List<Integer> resultList = (List<Integer>) workerResult.getResult();
                            aggregator.complete(workerTaskId, resultList);
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

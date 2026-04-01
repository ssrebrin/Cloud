package cloud.Cluster;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import cloud.domain.Operation;
import cloud.domain.TaskResult;
import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskStatus;
import cloud.serialization.CloudClassLoader;
import cloud.serialization.KryoSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker {

    private static final IFn CLOJURE_READ_STRING = Clojure.var("clojure.core", "read-string");
    private static final IFn CLOJURE_EVAL = Clojure.var("clojure.core", "eval");
    private static final long COMPLETED_TTL_MS = 60_000;

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

    private final Map<String, WorkerTaskStatus> activeTasks = new ConcurrentHashMap<>();
    private final Map<String, Long> completedTasks = new ConcurrentHashMap<>();

    public TaskResult process(WorkerTask task) {
        try {
            CloudClassLoader classLoader = new CloudClassLoader(this.getClass().getClassLoader());

            if (task.getJarBytes() != null) {
                byte[] jarBytes = java.util.Base64.getDecoder().decode(task.getJarBytes());
                classLoader.addJar(jarBytes);
            }

            if (task.isPipelineOp() && task.getOperation() != null) {
                return processPipelineOperation(task, classLoader);
            }

            if (isClojure(task.getLanguage())) {
                IFn fn = compileClojureFunction(task.getSerializedFunction());
                List<Integer> results = new ArrayList<>();
                for (Integer val : task.getValues()) {
                    results.add(asInt(fn.invoke(val)));
                }
                return new TaskResult<>(task.getTaskId(), results, null, task.getWorkerTaskId());
            }

            byte[] fnBytes = java.util.Base64.getDecoder().decode(task.getSerializedFunction());
            Object fn = serializer.deserialize(fnBytes, classLoader);

            List<Integer> results = new ArrayList<>();
            for (Integer val : task.getValues()) {
                results.add(asInt(invokeUnaryFunction(fn, val)));
            }

            return new TaskResult<>(task.getTaskId(), results, null, task.getWorkerTaskId());

        } catch (Exception e) {
            return new TaskResult<>(task.getTaskId(), null, e.getMessage(), task.getWorkerTaskId());
        }
    }

    private TaskResult processPipelineOperation(WorkerTask task, CloudClassLoader classLoader) {
        Operation op = task.getOperation();
        Object currentData = task.getCurrentData();

        System.out.println("Processing pipeline operation " + task.getOperationIndex() + "/" + task.getTotalOperations() +
                " of type: " + op.type + " [" + op.getLanguage() + "]");

        try {
            return switch (op.type.toLowerCase()) {
                case "map" -> handleMap(task, op, currentData, classLoader);
                case "filter" -> handleFilter(task, op, currentData, classLoader);
                case "reduce" -> handleReduce(task, op, currentData, classLoader);
                default -> throw new IllegalArgumentException("Unknown operation type: " + op.type);
            };
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return new TaskResult<>(task.getTaskId(), null, e.getMessage(), task.getWorkerTaskId());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TaskResult handleMap(WorkerTask task, Operation op, Object currentData, CloudClassLoader classLoader) throws Exception {
        if (isClojure(op.getLanguage())) {
            IFn fn = compileClojureOperation(op);
            if (currentData instanceof List<?> dataList) {
                List<Object> results = new ArrayList<>();
                for (Object item : dataList) {
                    results.add(fn.invoke(item));
                }
                System.out.println("Map operation completed, processed " + results.size() + " items");
                return new TaskResult<>(task.getTaskId(), results, null, task.getWorkerTaskId());
            }
            Object result = fn.invoke(currentData);
            return new TaskResult<>(task.getTaskId(), List.of(result), null, task.getWorkerTaskId());
        }

        Object fn = serializer.deserialize(op.function, classLoader);

        if (currentData instanceof List<?> dataList) {
            List<Object> results = new ArrayList<>();
            for (Object item : dataList) {
                results.add(invokeUnaryFunction(fn, item));
            }
            System.out.println("Map operation completed, processed " + results.size() + " items");
            return new TaskResult<>(task.getTaskId(), results, null, task.getWorkerTaskId());
        }

        Object result = invokeUnaryFunction(fn, currentData);
        return new TaskResult<>(task.getTaskId(), List.of(result), null, task.getWorkerTaskId());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TaskResult handleFilter(WorkerTask task, Operation op, Object currentData, CloudClassLoader classLoader) throws Exception {
        if (isClojure(op.getLanguage())) {
            IFn predicate = compileClojureOperation(op);
            if (currentData instanceof List<?> dataList) {
                List<Object> filtered = new ArrayList<>();
                for (Object item : dataList) {
                    if (isTruthy(predicate.invoke(item))) {
                        filtered.add(item);
                    }
                }
                System.out.println("Filter operation completed, kept " + filtered.size() + " of " + dataList.size() + " items");
                return new TaskResult<>(task.getTaskId(), filtered, null, task.getWorkerTaskId());
            }
            Object result = isTruthy(predicate.invoke(currentData)) ? currentData : null;
            return new TaskResult<>(task.getTaskId(), result, null, task.getWorkerTaskId());
        }

        Object predicate = serializer.deserialize(op.function, classLoader);

        if (currentData instanceof List<?> dataList) {
            List<Object> filtered = new ArrayList<>();
            for (Object item : dataList) {
                Boolean shouldInclude = asBoolean(invokeUnaryFunction(predicate, item));
                if (shouldInclude) {
                    filtered.add(item);
                }
            }
            System.out.println("Filter operation completed, kept " + filtered.size() + " of " + dataList.size() + " items");
            return new TaskResult<>(task.getTaskId(), filtered, null, task.getWorkerTaskId());
        }

        Boolean shouldInclude = asBoolean(invokeUnaryFunction(predicate, currentData));
        Object result = shouldInclude ? currentData : null;
        return new TaskResult<>(task.getTaskId(), result, null, task.getWorkerTaskId());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TaskResult handleReduce(WorkerTask task, Operation op, Object currentData, CloudClassLoader classLoader) throws Exception {
        if (isClojure(op.getLanguage())) {
            IFn reducer = compileClojureOperation(op);
            if (currentData instanceof List<?> dataList) {
                if (dataList.isEmpty()) {
                    return new TaskResult<>(task.getTaskId(), null, null, task.getWorkerTaskId());
                }
                Object result = dataList.get(0);
                for (int i = 1; i < dataList.size(); i++) {
                    result = reducer.invoke(result, dataList.get(i));
                }
                System.out.println("Reduce operation completed");
                return new TaskResult<>(task.getTaskId(), result, null, task.getWorkerTaskId());
            }
            return new TaskResult<>(task.getTaskId(), currentData, null, task.getWorkerTaskId());
        }

        Object reducer = serializer.deserialize(op.function, classLoader);

        if (currentData instanceof List<?> dataList) {
            if (dataList.isEmpty()) {
                return new TaskResult<>(task.getTaskId(), null, null, task.getWorkerTaskId());
            }

            Object result = dataList.get(0);
            for (int i = 1; i < dataList.size(); i++) {
                result = invokeBinaryFunction(reducer, result, dataList.get(i));
            }
            System.out.println("Reduce operation completed");
            return new TaskResult<>(task.getTaskId(), result, null, task.getWorkerTaskId());
        }

        return new TaskResult<>(task.getTaskId(), currentData, null, task.getWorkerTaskId());
    }

    private IFn compileClojureOperation(Operation op) {
        String source = new String(op.function, StandardCharsets.UTF_8);
        return compileClojureFunction(source);
    }

    private IFn compileClojureFunction(String source) {
        Object form = CLOJURE_READ_STRING.invoke(source);
        Object value = CLOJURE_EVAL.invoke(form);
        if (!(value instanceof IFn fn)) {
            throw new IllegalArgumentException("Clojure form must evaluate to a function");
        }
        return fn;
    }

    private boolean isClojure(String language) {
        return language != null && language.equalsIgnoreCase("clojure");
    }

    private boolean isTruthy(Object value) {
        return value != null && !Boolean.FALSE.equals(value);
    }

    private int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new IllegalArgumentException("Clojure function must return Number, got: " + value);
    }

    private Boolean asBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("Filter function must return Boolean, got: " + value);
    }

    private Object invokeUnaryFunction(Object fn, Object value) {
        return invokeApply(fn, value);
    }

    private Object invokeBinaryFunction(Object fn, Object left, Object right) {
        return invokeApply(fn, left, right);
    }

    private Object invokeApply(Object fn, Object... args) {
        try {
            Method applyMethod = findApplyMethod(fn.getClass(), args.length);
            return applyMethod.invoke(fn, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RuntimeException("Remote function invocation failed: " + cause.getMessage(), cause);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke remote function: " + e.getMessage(), e);
        }
    }

    private Method findApplyMethod(Class<?> type, int arity) throws NoSuchMethodException {
        Method fallback = null;
        for (Method method : type.getMethods()) {
            if (!"apply".equals(method.getName())) {
                continue;
            }
            if (method.getParameterCount() == arity) {
                method.setAccessible(true);
                return method;
            }
            if (fallback == null) {
                fallback = method;
            }
        }
        if (fallback != null) {
            fallback.setAccessible(true);
            return fallback;
        }
        throw new NoSuchMethodException("No apply method with arity " + arity + " in " + type.getName());
    }

    public void startWorkers(int threads) {
        workerPool = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            workerPool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        WorkerTask task = Tasks.take();

                        activeTasks.put(task.getWorkerTaskId(), WorkerTaskStatus.PENDING);
                        TaskResult result = process(task);

                        activeTasks.remove(task.getWorkerTaskId());
                        completedTasks.put(task.getWorkerTaskId(), System.currentTimeMillis());

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
        server.createContext("/health", new HealthHandler());
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

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");

            if (parts.length < 3) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            String workerTaskId = parts[2];

            if (activeTasks.containsKey(workerTaskId)) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }

            if (completedTasks.containsKey(workerTaskId)) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void startCleanupThread() {
        Thread cleaner = new Thread(() -> {
            while (true) {
                try {
                    long now = System.currentTimeMillis();

                    completedTasks.entrySet().removeIf(entry ->
                            now - entry.getValue() > COMPLETED_TTL_MS
                    );

                    Thread.sleep(5000);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        cleaner.setDaemon(true);
        cleaner.start();
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
        worker.startCleanupThread();
        worker.startResultSender();
    }
}

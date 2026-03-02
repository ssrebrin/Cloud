package cloud.CloudManager;

import cloud.RemoteFunction;
import cloud.Task;
import cloud.TaskResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Network implements Runnable {
    private BlockingQueue<Task> outgoingTasks;
    private final BlockingQueue<TaskResult> incomingResults;
    private final Map<String, ClusterInfo> clusters;

    public Network(BlockingQueue<Task> tasks, BlockingQueue<TaskResult> results,
                   Map<String, ClusterInfo> clusters
    ) {
        this.outgoingTasks = tasks;
        this.incomingResults = results;
        this.clusters = clusters;
    }

    static class ExecuteHandler implements HttpHandler {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            // Читаем тело запроса
            InputStream is = exchange.getRequestBody();
            String body = new String(is.readAllBytes());
            Map<String, Object> requestMap = mapper.readValue(body, Map.class);

            try {
                // Получаем функцию
                String fnBase64 = (String) requestMap.get("function");
                byte[] fnBytes = Base64.getDecoder().decode(fnBase64);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(fnBytes));
                RemoteFunction<Object, Object> fn = (RemoteFunction<Object, Object>) ois.readObject();

                // Получаем аргумент
                Map<String, Object> argMap = (Map<String, Object>) requestMap.get("argument");
                Object arg = argMap.get("value");

                Object result = fn.apply(arg);

                // Сериализуем обратно
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(result);
                oos.flush();
                String encodedResult = Base64.getEncoder().encodeToString(bos.toByteArray());

                Map<String, Object> responseMap = Map.of("result", encodedResult);
                String jsonResponse = mapper.writeValueAsString(responseMap);

                // Отправляем ответ
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(jsonResponse.getBytes());
                os.close();

            } catch (Exception e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    static class RegisterHandler implements HttpHandler {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Map<String, ClusterInfo> clusters;

        public RegisterHandler(Map<String, ClusterInfo> clusters) {
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
            int port = (Integer) request.get("port");

            ClusterInfo cluster = new ClusterInfo(id, host, port);
            clusters.put(id, cluster);

            System.out.println("Cluster registered: " + id);

            exchange.sendResponseHeaders(200, -1);
        }
    }

    @Override
    public void run() {

        int port = 8080;
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        server.createContext("/execute", new ExecuteHandler());
        server.createContext("/register", new RegisterHandler(this.clusters));
        server.setExecutor(null); // создаёт поток по умолчанию
        server.start();
        System.out.println("Server started at http://localhost:" + port);
    }
}

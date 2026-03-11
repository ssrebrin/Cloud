package cloud.Cluster;

import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;
import cloud.cloud.RemoteFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

public class Cluster {

    private final String managerHost;
    private final int managerPort;
    public final String hostName;
    public final int port;
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
                    "host", this.hostName,
                    "port", this.port
            );

            String json = mapper.writeValueAsString(payload);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes());
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Cluster registered successfully: " + id);
                return true;
            } else {
                System.out.println("Failed to register cluster. Response code: " + responseCode);
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void startServer() throws IOException {
        if (!register()) {
            throw new RuntimeException("Failed to register cluster!");
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(this.port), 0);
        server.createContext("/execute", new ExecuteHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        System.out.println("Cluster server started on port " + this.port);
    }

    private class ExecuteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                String body = new String(exchange.getRequestBody().readAllBytes());
                Map<String, Object> requestMap = mapper.readValue(body, Map.class);

                String fnBase64 = (String) requestMap.get("function");
                byte[] fnBytes = Base64.getDecoder().decode(fnBase64);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(fnBytes));
                RemoteFunction<Object, Object> fn = (RemoteFunction<Object, Object>) ois.readObject();

                Map<String, Object> argMap = (Map<String, Object>) requestMap.get("argument");
                Object arg = argMap.get("value");

                /*Task<java. io. Serializable, Object> task = new Task<>(
                        (RemoteFunction<java.io.Serializable, Object>) fn,
                        arg
                );


                worker.execute(new Task<>(fn, arg), (TaskResult<Object> result) -> {
                    try {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(result.getResult()); // <-- TaskResult использует getResult()
                        oos.flush();

                        String encodedResult = Base64.getEncoder().encodeToString(bos.toByteArray());
                        Map<String, Object> responseMap = Map.of("result", encodedResult);
                        String jsonResponse = mapper.writeValueAsString(responseMap);

                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(jsonResponse.getBytes());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });*/

            } catch (Exception e) {
                e.printStackTrace();
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Cluster cluster = new Cluster("localhost", 8085, "localhost", 8086);
        cluster.register();
        //cluster.startServer();
    }
}
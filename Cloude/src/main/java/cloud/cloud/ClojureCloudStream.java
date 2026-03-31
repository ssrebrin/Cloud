package cloud.cloud;

import cloud.domain.Operation;
import cloud.domain.Task;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClojureCloudStream {
    private final String managerUrl;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private List<Object> values;
    private final List<Operation> ops = new ArrayList<>();

    private ClojureCloudStream(String managerUrl) {
        this.managerUrl = managerUrl;
        this.client = HttpClient.newHttpClient();
        this.mapper = new ObjectMapper();
    }

    public static ClojureCloudStream connect(String url) {
        return new ClojureCloudStream(url);
    }

    public ClojureCloudStream stream(List<?> values) {
        this.values = new ArrayList<>(values);
        return this;
    }

    public ClojureCloudStream stream(int[] values) {
        List<Integer> boxed = Arrays.stream(values).boxed().toList();
        this.values = new ArrayList<>(boxed);
        return this;
    }

    public ClojureCloudStream map(String clojureFn) {
        ops.add(new Operation("map", clojureFn.getBytes(StandardCharsets.UTF_8), Task.LANGUAGE_CLOJURE));
        return this;
    }

    public ClojureCloudStream filter(String clojurePredicate) {
        ops.add(new Operation("filter", clojurePredicate.getBytes(StandardCharsets.UTF_8), Task.LANGUAGE_CLOJURE));
        return this;
    }

    public ClojureCloudStream reduce(String clojureReducer) {
        ops.add(new Operation("reduce", clojureReducer.getBytes(StandardCharsets.UTF_8), Task.LANGUAGE_CLOJURE));
        return this;
    }

    public List<Object> execute() throws IOException, InterruptedException {
        if (values == null) {
            throw new IllegalStateException("No input data. Call stream(...) before execute().");
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("ops", ops);
        requestBody.put("data", values);
        requestBody.put("values", values);
        requestBody.put("callback", "http://localhost:8087/callback");
        requestBody.put("language", Task.LANGUAGE_CLOJURE);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(managerUrl + "/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(requestBody)))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Map<String, Object> responseMap = mapper.readValue(response.body(), Map.class);

        if (!"accepted".equals(responseMap.get("status"))) {
            throw new RuntimeException("Task was not accepted");
        }

        String taskId = String.valueOf(responseMap.get("taskId"));
        ops.clear();
        return waitForResult(taskId);
    }

    @SuppressWarnings("unchecked")
    private List<Object> waitForResult(String taskId) throws IOException, InterruptedException {
        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(managerUrl + "/result/" + taskId))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<String, Object> resultMap = mapper.readValue(response.body(), Map.class);
            String status = String.valueOf(resultMap.get("status"));

            if ("done".equals(status)) {
                Object raw = resultMap.get("result");
                if (raw == null) {
                    return List.of();
                }
                if (raw instanceof List<?> list) {
                    return (List<Object>) list;
                }
                return List.of(raw);
            }

            if ("error".equals(status)) {
                throw new RuntimeException(String.valueOf(resultMap.get("error")));
            }

            Thread.sleep(500);
        }
    }
}

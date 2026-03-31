package cloud.cloud;

import cloud.domain.Task;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClojureCloud {

    private final String managerUrl;
    private final HttpClient client;
    private final ObjectMapper mapper;

    private ClojureCloud(String managerUrl) {
        this.managerUrl = managerUrl;
        this.client = HttpClient.newHttpClient();
        this.mapper = new ObjectMapper();
    }

    public static ClojureCloud connect(String url) {
        return new ClojureCloud(url);
    }

    public List<Integer> execute(String clojureFn, int[] values) throws IOException, InterruptedException {
        List<Integer> payloadValues = Arrays.stream(values).boxed().toList();

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("serializedFunction", clojureFn);
        requestBody.put("values", payloadValues);
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
        return waitForResult(taskId);
    }

    private List<Integer> waitForResult(String taskId) throws IOException, InterruptedException {
        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(managerUrl + "/result/" + taskId))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<String, Object> resultMap = mapper.readValue(response.body(), Map.class);
            String status = String.valueOf(resultMap.get("status"));

            if ("done".equals(status)) {
                return parseIntegerList(resultMap.get("result"));
            }

            if ("error".equals(status)) {
                throw new RuntimeException(String.valueOf(resultMap.get("error")));
            }

            Thread.sleep(500);
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
}

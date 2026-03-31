package cloud.cloud;

import cloud.domain.RemoteFunction;
import cloud.serialization.AnnotationScanner;
import cloud.serialization.CodePacker;
import cloud.serialization.KryoSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class Cloud {

    private final String managerUrl;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final KryoSerializer serializer;

    private Cloud(String managerUrl) {
        this.managerUrl = managerUrl;
        this.client = HttpClient.newHttpClient();
        this.mapper = new ObjectMapper();
        this.serializer = new KryoSerializer();
    }

    public static Cloud connect(String url) {
        return new Cloud(url);
    }

    public List<Integer> execute(RemoteFunction<Integer, Integer> fn, int[] values, Class<?>... extraClasses)
            throws IOException, InterruptedException {

        List<Integer> payloadValues = Arrays.stream(values).boxed().toList();

        byte[] serializedFn = serializer.serialize(fn);
        
        // Объединяем обязательные классы, дополнительные и аннотированные
        Class<?>[] annotatedClasses = AnnotationScanner.findAnnotatedClasses();
        Class<?>[] allForJar = new Class<?>[extraClasses.length + annotatedClasses.length + 3];
        allForJar[0] = fn.getClass();
        allForJar[1] = Main.class;
        allForJar[2] = RemoteFunction.class;
        System.arraycopy(extraClasses, 0, allForJar, 3, extraClasses.length);
        System.arraycopy(annotatedClasses, 0, allForJar, 3 + extraClasses.length, annotatedClasses.length);
        
        byte[] jarBytes = CodePacker.packClass(allForJar);

        Map<String, Object> requestBody = Map.of(
                "serializedFunction", Base64.getEncoder().encodeToString(serializedFn),
                "jarBytes", Base64.getEncoder().encodeToString(jarBytes),
                "values", payloadValues,
                "callback", "http://localhost:8087/callback"
        );

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

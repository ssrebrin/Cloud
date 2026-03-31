package cloud.cloud;

import cloud.domain.RemoteFunction;
import cloud.domain.RemoteReducer;
import cloud.domain.Operation;
import cloud.domain.Task;
import cloud.serialization.AnnotationScanner;
import cloud.serialization.CodePacker;
import cloud.serialization.KryoSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class CloudStream<T extends Serializable> {
    private final String managerUrl;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final KryoSerializer serializer;
    private List<T> values;
    List<Operation> ops = new ArrayList<>();


    private CloudStream(String managerUrl) {
        this.managerUrl = managerUrl;
        this.client = HttpClient.newHttpClient();
        this.mapper = new ObjectMapper();
        this.serializer = new KryoSerializer();
    }

    public static <T extends Serializable> CloudStream<T> connect(String url) {
        return new CloudStream(url);
    }

    public CloudStream<T> stream(List<T> values) {
        this.values = new ArrayList<>(values);
        return this;
    }

    public CloudStream<T> stream(T[] values) {
        this.values = new ArrayList<>(Arrays.asList(values));
        return this;
    }

    @SuppressWarnings("unchecked")
    public CloudStream<T> stream(int[] values) {
        List<Integer> boxed = Arrays.stream(values).boxed().toList();
        this.values = (List<T>) boxed;
        return this;
    }

    public CloudStream<T> SetData(List<T> values) {
        return stream(values);
    }

    public CloudStream<T> SetData(T[] values) {
        return stream(values);
    }

    public CloudStream<T> SetData(int[] values) {
        return stream(values);
    }

    public <R extends Serializable> CloudStream<R> map(RemoteFunction<T, R> f) {
        ops.add(new Operation("map", serializer.serialize(f), Task.LANGUAGE_JAVA));
        return (CloudStream <R>) this;
    }

    public CloudStream<T> filter(RemoteFunction<T, Boolean> f) {
        ops.add(new Operation("filter", serializer.serialize(f), Task.LANGUAGE_JAVA));
        return this;
    }


    public CloudStream<T> reduce(RemoteReducer<T> f) {
        ops.add(new Operation("reduce", serializer.serialize(f), Task.LANGUAGE_JAVA));
        return this;
    }

    public List<T> execute(Class<?>... extraClasses)
            throws IOException, InterruptedException {

        if (values == null) {
            throw new IllegalStateException("No input data. Call stream(...) before execute().");
        }

        List<Object> payloadValues = new ArrayList<>(values);

        // Объединяем обязательные классы, дополнительные и аннотированные
        Class<?>[] annotatedClasses = AnnotationScanner.findAnnotatedClasses();
        Class<?>[] allForJar = new Class<?>[extraClasses.length + annotatedClasses.length + 4];
        allForJar[0] = Main.class;
        allForJar[1] = RemoteFunction.class;
        allForJar[2] = RemoteReducer.class;
        allForJar[3] = Operation.class;
        System.arraycopy(extraClasses, 0, allForJar, 4, extraClasses.length);
        System.arraycopy(annotatedClasses, 0, allForJar, 4 + extraClasses.length, annotatedClasses.length);

        byte[] jarBytes = CodePacker.packClass(allForJar);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("ops", ops);
        requestBody.put("data", payloadValues);
        requestBody.put("jarBytes", Base64.getEncoder().encodeToString(jarBytes));
        requestBody.put("values", payloadValues);
        requestBody.put("callback", "http://localhost:8087/callback");
        requestBody.put("language", Task.LANGUAGE_JAVA);

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
        
        // Очищаем ops после отправки для возможности переиспользования
        ops.clear();
        
        return waitForResult(taskId);
    }

    private List<T> waitForResult(String taskId) throws IOException, InterruptedException {
        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(managerUrl + "/result/" + taskId))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<String, Object> resultMap = mapper.readValue(response.body(), Map.class);
            String status = String.valueOf(resultMap.get("status"));

            if ("done".equals(status)) {
                return parseResultList(resultMap.get("result"));
            }

            if ("error".equals(status)) {
                throw new RuntimeException(String.valueOf(resultMap.get("error")));
            }

            Thread.sleep(500);
        }
    }

    @SuppressWarnings("unchecked")
    private List<T> parseResultList(Object rawList) {
        if (rawList == null) {
            return List.of();
        }
        if (rawList instanceof List<?> list) {
            return (List<T>) list;
        }
        return List.of((T) rawList);
    }
}

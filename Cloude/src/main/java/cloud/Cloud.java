package cloud;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Cloud {
    private final String managerUrl;
    private HttpClient client;

    private Cloud(String managerUrl) {
        this.managerUrl = managerUrl;
    }

    public static Cloud connect(String url) {
        Cloud cloud = new Cloud(url);
        cloud.client = HttpClient.newHttpClient();
        return cloud;
    }

    private static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        out.flush();
        return bos.toByteArray();
    }

    public <T extends Serializable, R>
    R execute(RemoteFunction<T, R> fn, T arg) throws IOException, InterruptedException, ClassNotFoundException {

        byte[] bytes = serialize(fn);
        String encoded = Base64.getEncoder().encodeToString(bytes);

        Map<String, Object> req = Map.of(
                "function", encoded,
                "argument", Map.of("value", arg)
        );

        ObjectMapper mapper = new ObjectMapper();

        String json = mapper.writeValueAsString(req);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(managerUrl + "/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        String body = response.body();

        Map<String, Object> map =
                mapper.readValue(body, Map.class);

        String encodedRes = map.get("result").toString();

        byte[] decoded = Base64.getDecoder().decode(encodedRes);

        ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(decoded));

        return (R) in.readObject();
    }
}

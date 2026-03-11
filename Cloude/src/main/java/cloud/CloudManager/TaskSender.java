package cloud.CloudManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import cloud.cloud.RemoteFunction;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

public class TaskSender {

    private final ObjectMapper mapper = new ObjectMapper();

    public <T extends Serializable, R> R sendTask(String clusterHost, int clusterPort, cloud.CloudManager.Task<T, R> task) throws Exception {

        URL url = new URL("http://" + clusterHost + ":" + clusterPort + "/execute");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        // сериализация RemoteFunction
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(task.getFunction());
        }
        String fnBase64 = Base64.getEncoder().encodeToString(bos.toByteArray());

        Map<String, Object> payload = Map.of(
                "id", task.getId(),
                "function", fnBase64,
                "argument", Map.of("value", task.getArgument())
        );

        String json = mapper.writeValueAsString(payload);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes());
        }

        int code = conn.getResponseCode();
        if (code != 200) {
            throw new RuntimeException("Cluster returned code " + code);
        }

        Map<String, Object> response = mapper.readValue(conn.getInputStream(), Map.class);
        String resultBase64 = (String) response.get("result");

        byte[] resultBytes = Base64.getDecoder().decode(resultBase64);
        Object result;
        try (ObjectInputStream ois = new ObjectInputStream(new java.io.ByteArrayInputStream(resultBytes))) {
            result = ois.readObject();
        }

        return (R) result;
    }
}
package cloud.CloudManager;

import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class TaskSender {

    private final ObjectMapper mapper = new ObjectMapper();

    public String sendTask(String clusterHost, int clusterPort, WorkerTask task) throws Exception {
        URL url = new URL("http://" + clusterHost + ":" + clusterPort + "/execute");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        String json = mapper.writeValueAsString(task);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes());
        }

        int code = conn.getResponseCode();

        if (code == 202) {
            return "";
        }

        String error;
        try (InputStream err = conn.getErrorStream()) {
            if (err != null) {
                byte[] payload = err.readAllBytes();
                String raw = new String(payload, StandardCharsets.UTF_8);
                try {
                    Map<String, Object> response = mapper.readValue(payload, Map.class);
                    error = String.valueOf(response.get("error"));
                } catch (Exception parseError) {
                    error = raw.isBlank() ? ("Unknown error, code=" + code) : raw;
                }
            } else {
                error = "Unknown error, code=" + code;
            }
        }

        return error;
    }
    private List<Integer> parseIntegerList(Object rawList) {
        if (!(rawList instanceof List<?> list)) {
            return List.of();
        }

        return list.stream()
                .map(value -> ((Number) value).intValue())
                .toList();
    }

    public WorkerTaskStatus checkStatus(String host, int port, String workerTaskId) {
        try {
            URL url = new URL("http://" + host + ":" + port + "/health/" + workerTaskId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");

            int code = conn.getResponseCode();

            if (code == 200) return WorkerTaskStatus.PENDING;
            if (code == 204) return WorkerTaskStatus.DONE;
            if (code == 404) return WorkerTaskStatus.NOT_FOUND;

        } catch (Exception e) {
            return WorkerTaskStatus.NOT_FOUND;
        }

        return WorkerTaskStatus.NOT_FOUND;
    }
}

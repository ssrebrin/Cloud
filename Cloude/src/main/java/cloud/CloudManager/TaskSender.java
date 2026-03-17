package cloud.CloudManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class TaskSender {

    private final ObjectMapper mapper = new ObjectMapper();

    public TaskResult<List<Integer>> sendTask(String clusterHost, int clusterPort, Task task) throws Exception {
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
        if (code != 200) {
            throw new RuntimeException("Cluster returned code " + code);
        }

        Map<String, Object> response = mapper.readValue(conn.getInputStream(), Map.class);
        String status = String.valueOf(response.get("status"));
        String taskId = String.valueOf(response.get("taskId"));

        if ("done".equals(status)) {
            List<Integer> values = parseIntegerList(response.get("result"));
            return new TaskResult<>(taskId, values);
        }

        return new TaskResult<>(taskId, String.valueOf(response.get("error")));
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

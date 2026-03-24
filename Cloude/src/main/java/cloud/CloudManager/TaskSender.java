package cloud.CloudManager;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.OutputStream;
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
                System.out.println("Error: " + err);
                Map<String, Object> response = mapper.readValue(err, Map.class);
                error = String.valueOf(response.get("error"));
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
}

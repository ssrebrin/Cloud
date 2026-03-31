package cloud.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskResult<R> implements Serializable {

    private String taskId;
    private R result;
    private String error;
    private String workerTaskId;

    public TaskResult() {}

    public TaskResult(String taskId, R result, String error, String workerTaskId) {
        this.taskId = taskId;
        this.result = result;
        this.error = error;
        this.workerTaskId = workerTaskId;
    }


    public String getTaskId() {
        return taskId;
    }

    public String getWorkerTaskId() {
        return workerTaskId;
    }

    public R getResult() {
        return result;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isSuccess() {
        return error == null;
    }
}

package cloud.CloudManager;

import java.io.Serializable;

public class TaskResult<R> implements Serializable {

    private final String taskId;
    private final R result;
    private String error;

    public TaskResult(String taskId, R result, String error) {
        this.taskId = taskId;
        this.result = result;
        this.error = error;
    }


    public String getTaskId() {
        return taskId;
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
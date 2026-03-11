package cloud.CloudManager;

import java.io.Serializable;

public class TaskResult<R> implements Serializable {

    private final String taskId;
    private final R result;
    private final String error;

    public TaskResult(String taskId, R result) {
        this.taskId = taskId;
        this.result = result;
        this.error = null;
    }

    public TaskResult(String taskId, String error) {
        this.taskId = taskId;
        this.result = null;
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

    public boolean isSuccess() {
        return error == null;
    }
}
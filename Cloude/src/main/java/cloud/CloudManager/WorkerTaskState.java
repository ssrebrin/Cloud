package cloud.CloudManager;

import java.util.List;

public class WorkerTaskState {
    private final WorkerTask task;
    private WorkerTaskStatus status;
    private List<Integer> result;
    private String error;

    public WorkerTaskState(WorkerTask task) {
        this.task = task;
        this.status = WorkerTaskStatus.PENDING;
    }

    public WorkerTask getTask() {
        return task;
    }

    public WorkerTaskStatus getStatus() {
        return status;
    }

    public void markDone(List<Integer> result) {
        this.status = WorkerTaskStatus.DONE;
        this.result = result;
    }

    public void markError(String error) {
        this.status = WorkerTaskStatus.ERROR;
        this.error = error;
    }

    public List<Integer> getResult() {
        return result;
    }

    public String getError() {
        return error;
    }
}
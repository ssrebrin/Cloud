package cloud.CloudManager;

import cloud.domain.TaskResult;
import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskState;
import cloud.domain.WorkerTaskStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskAggregator {

    private final String taskId;

    // workerTaskId -> state
    private final Map<String, WorkerTaskState> tasks = new ConcurrentHashMap<>();

    private final AtomicInteger remaining;

    public TaskAggregator(String taskId, List<WorkerTask> workerTasks) {
        this.taskId = taskId;
        this.remaining = new AtomicInteger(workerTasks.size());

        for (WorkerTask wt : workerTasks) {
            tasks.put(wt.getWorkerTaskId(), new WorkerTaskState(wt));
        }
    }

    // ✅ вызывается при получении результата от воркера
    public void complete(String workerTaskId, List<Integer> result) {
        WorkerTaskState state = tasks.get(workerTaskId);
        if (state == null) return;

        if (state.getStatus() == WorkerTaskStatus.PENDING) {
            state.markDone(result);
            remaining.decrementAndGet();
        }
    }

    public void fail(String workerTaskId, String error) {
        WorkerTaskState state = tasks.get(workerTaskId);
        if (state == null) return;

        if (state.getStatus() == WorkerTaskStatus.PENDING) {
            state.markError(error);
            remaining.decrementAndGet();
        }
    }

    // ✅ все ли завершены
    public boolean isFinished() {
        return remaining.get() == 0;
    }

    // ✅ собрать финальный результат
    public TaskResult<List<Integer>> buildResult() {

        List<Integer> finalResult = new ArrayList<>();

        for (WorkerTaskState state : tasks.values()) {

            if (state.getStatus() == WorkerTaskStatus.ERROR) {
                return new TaskResult<>(taskId, null, state.getError(), "");
            }

            if (state.getResult() != null) {
                finalResult.addAll(state.getResult());
            }
        }

        return new TaskResult<>(taskId, finalResult, null, "");
    }
}
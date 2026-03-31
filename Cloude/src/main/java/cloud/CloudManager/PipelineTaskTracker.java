package cloud.CloudManager;

import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks single pipeline operations (not batched like TaskAggregator).
 * Each pipeline operation is a single WorkerTask that executes sequentially.
 */
public class PipelineTaskTracker {

    // workerTaskId -> status
    private final Map<String, WorkerTaskStatus> taskStatuses = new ConcurrentHashMap<>();

    // workerTaskId -> WorkerTask
    private final Map<String, WorkerTask> tasks = new ConcurrentHashMap<>();

    // taskId -> current pipeline state
    private final Map<String, PipelineState> pipelineStates = new ConcurrentHashMap<>();

    public void register(WorkerTask task) {
        String workerTaskId = task.getWorkerTaskId();
        tasks.put(workerTaskId, task);
        taskStatuses.put(workerTaskId, WorkerTaskStatus.PENDING);

        // Track pipeline state
        PipelineState state = pipelineStates.computeIfAbsent(
                task.getTaskId(),
                k -> new PipelineState(task.getTotalOperations())
        );
        state.registerOperation(task.getOperationIndex(), workerTaskId);
    }

    public void complete(String workerTaskId) {
        taskStatuses.put(workerTaskId, WorkerTaskStatus.DONE);

        WorkerTask task = tasks.get(workerTaskId);
        if (task != null) {
            PipelineState state = pipelineStates.get(task.getTaskId());
            if (state != null) {
                state.markComplete(task.getOperationIndex());
            }
        }
    }

    public void fail(String workerTaskId) {
        taskStatuses.put(workerTaskId, WorkerTaskStatus.ERROR);
    }

    public WorkerTaskStatus getStatus(String workerTaskId) {
        return taskStatuses.getOrDefault(workerTaskId, WorkerTaskStatus.NOT_FOUND);
    }

    public WorkerTask getTask(String workerTaskId) {
        return tasks.get(workerTaskId);
    }

    public boolean isFinished(String workerTaskId) {
        WorkerTaskStatus status = taskStatuses.get(workerTaskId);
        return status == WorkerTaskStatus.DONE || status == WorkerTaskStatus.ERROR;
    }

    public boolean isPipelineComplete(String taskId) {
        PipelineState state = pipelineStates.get(taskId);
        return state != null && state.isComplete();
    }

    public void cleanup(String workerTaskId) {
        WorkerTask task = tasks.remove(workerTaskId);
        taskStatuses.remove(workerTaskId);

        // Cleanup pipeline state when all operations done
        if (task != null) {
            PipelineState state = pipelineStates.get(task.getTaskId());
            if (state != null && state.isComplete()) {
                pipelineStates.remove(task.getTaskId());
            }
        }
    }

    public void cleanupTask(String taskId) {
        PipelineState state = pipelineStates.remove(taskId);
        if (state != null) {
            for (String workerTaskId : state.getAllWorkerTaskIds()) {
                tasks.remove(workerTaskId);
                taskStatuses.remove(workerTaskId);
            }
        }
    }

    public String getTaskForOperation(String taskId, int operationIndex) {
        PipelineState state = pipelineStates.get(taskId);
        if (state != null) {
            return state.getWorkerTaskIdForOperation(operationIndex);
        }
        return null;
    }

    /**
     * Internal class to track state of a single pipeline (all its operations)
     */
    private static class PipelineState {
        private final int totalOperations;
        private final Map<Integer, String> operationIndexToWorkerTaskId = new ConcurrentHashMap<>();
        private final Map<Integer, Boolean> completedOperations = new ConcurrentHashMap<>();

        PipelineState(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        void registerOperation(int index, String workerTaskId) {
            operationIndexToWorkerTaskId.put(index, workerTaskId);
        }

        void markComplete(int index) {
            completedOperations.put(index, true);
        }

        boolean isComplete() {
            return completedOperations.size() >= totalOperations;
        }

        String getWorkerTaskIdForOperation(int index) {
            return operationIndexToWorkerTaskId.get(index);
        }

        java.util.Collection<String> getAllWorkerTaskIds() {
            return operationIndexToWorkerTaskId.values();
        }
    }
}

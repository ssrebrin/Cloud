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
                state.markComplete(task.getOperationIndex(), workerTaskId);
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
        private final Map<Integer, java.util.Set<String>> operationIndexToWorkerTaskIds = new ConcurrentHashMap<>();
        private final Map<Integer, java.util.Set<String>> completedOperationTasks = new ConcurrentHashMap<>();

        PipelineState(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        void registerOperation(int index, String workerTaskId) {
            operationIndexToWorkerTaskIds
                    .computeIfAbsent(index, k -> ConcurrentHashMap.newKeySet())
                    .add(workerTaskId);
        }

        void markComplete(int index, String workerTaskId) {
            completedOperationTasks
                    .computeIfAbsent(index, k -> ConcurrentHashMap.newKeySet())
                    .add(workerTaskId);
        }

        boolean isComplete() {
            if (operationIndexToWorkerTaskIds.isEmpty()) {
                return false;
            }
            for (int i = 0; i < totalOperations; i++) {
                if (!isOperationComplete(i)) {
                    return false;
                }
            }
            return true;
        }

        String getWorkerTaskIdForOperation(int index) {
            java.util.Set<String> ids = operationIndexToWorkerTaskIds.get(index);
            if (ids == null || ids.isEmpty()) {
                return null;
            }
            return ids.iterator().next();
        }

        java.util.Collection<String> getAllWorkerTaskIds() {
            java.util.List<String> result = new java.util.ArrayList<>();
            for (java.util.Set<String> ids : operationIndexToWorkerTaskIds.values()) {
                result.addAll(ids);
            }
            return result;
        }

        private boolean isOperationComplete(int index) {
            java.util.Set<String> registered = operationIndexToWorkerTaskIds.get(index);
            if (registered == null || registered.isEmpty()) {
                return false;
            }
            java.util.Set<String> completed = completedOperationTasks.get(index);
            return completed != null && completed.containsAll(registered);
        }
    }
}

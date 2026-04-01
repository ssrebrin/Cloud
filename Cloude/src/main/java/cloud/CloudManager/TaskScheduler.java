package cloud.CloudManager;

import cloud.domain.ClusterInfo;
import cloud.domain.Operation;
import cloud.domain.Task;
import cloud.domain.TaskResult;
import cloud.domain.WorkerTask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {

    private static final int BATCH_SIZE = 5;

    private final BlockingQueue<Task> outgoingTasks;
    private final BlockingQueue<WorkerTask> resendQueue;
    private final ConcurrentHashMap<String, ClusterInfo> clusters;
    private final ConcurrentHashMap<String, TaskResult<?>> completedResults;
    private final ConcurrentHashMap<String, TaskAggregator> aggregators;
    private final ConcurrentHashMap<String, ReduceState> reduceStates;
    // Track pipeline execution state: taskId -> current data for next operation
    private final ConcurrentHashMap<String, Object> pipelineData;
    // Track active (sent) tasks for pipeline lookup
    private final ConcurrentHashMap<String, Task> activeTasks;
    // Track pipeline tasks for health checking and retry
    private final PipelineTaskTracker pipelineTracker;
    private final Network network;
    private final TaskSender taskSender;
    private final ExecutorService dispatcher;
    private int currentIndex = 0;
    private HealthChecker healthChecker;

    public TaskScheduler(int managerPort) {
        this.outgoingTasks = new LinkedBlockingQueue<>();
        this.resendQueue = new LinkedBlockingQueue<>();
        this.clusters = new ConcurrentHashMap<>();
        this.completedResults = new ConcurrentHashMap<>();
        this.aggregators = new ConcurrentHashMap<>();
        this.reduceStates = new ConcurrentHashMap<>();
        this.pipelineData = new ConcurrentHashMap<>();
        this.activeTasks = new ConcurrentHashMap<>();
        this.pipelineTracker = new PipelineTaskTracker();
        this.network = new Network(outgoingTasks, clusters, completedResults, aggregators, managerPort, this);
        this.taskSender = new TaskSender();
        this.dispatcher = Executors.newSingleThreadExecutor();
        this.healthChecker = new HealthChecker(aggregators, pipelineTracker, taskSender, this::enqueueForResend);

        new Thread(network).start();
        dispatcher.submit(this::dispatchLoop);
        this.healthChecker.start();
    }

    public static List<WorkerTask> splitTask(Task task, int batchSize) {
        List<WorkerTask> result = new ArrayList<>();

        List<Integer> values = task.getValues();

        for (int i = 0; i < values.size(); i += batchSize) {
            int end = Math.min(i + batchSize, values.size());

            List<Integer> batch = values.subList(i, end);

            WorkerTask wt = new WorkerTask(
                    task.getId(),
                    task.getFunctionStub(),
                    batch
            );
            wt.setSerializedFunction(task.getSerializedFunction());
            wt.setJarBytes(task.getJarBytes());
            wt.setLanguage(task.getLanguage());
            result.add(wt);
        }

        return result;
    }
    
    public Task getTask(String taskId) {
        // First check active (sent) tasks
        Task activeTask = activeTasks.get(taskId);
        if (activeTask != null) {
            return activeTask;
        }
        // Then check outgoing queue
        for (Task task : outgoingTasks) {
            if (task.getId().equals(taskId)) {
                return task;
            }
        }
        return null;
    }

    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            Task task = null;

            try {
                WorkerTask workerTaskForResend = resendQueue.poll();
                if (workerTaskForResend != null) {
                    Task parentTask = getTask(workerTaskForResend.getTaskId());
                    if (parentTask != null) {
                        sendWorkerTask(workerTaskForResend, parentTask);
                    } else {
                        System.out.println("Drop resend task because parent not found: " + workerTaskForResend.getWorkerTaskId());
                    }
                    continue;
                }

                task = outgoingTasks.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                System.out.println("New task received: " + task.getId());
                
                if (task.isPipeline()) {
                    // Handle pipeline task - send first operation
                    System.out.println("Processing pipeline task with " + task.getOps().size() + " operations");
                    // Store task as active for pipeline tracking
                    activeTasks.put(task.getId(), task);
                    sendPipelineOperation(task, task.getInitialData());
                } else {
                    // Handle legacy task - split and send as before
                    List<WorkerTask> workerTasks = splitTask(task, BATCH_SIZE);
                    TaskAggregator aggregator = new TaskAggregator(task.getId(), workerTasks);
                    aggregators.put(task.getId(), aggregator);
                    // Store as active task
                    activeTasks.put(task.getId(), task);

                    for (WorkerTask wt : workerTasks) {
                        sendWorkerTask(wt, task);
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (task != null) {
                    completedResults.put(
                            task.getId(),
                            new TaskResult<>(task.getId(), null, e.getMessage(), "")
                    );
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

    private void enqueueForResend(WorkerTask workerTask) {
        if (workerTask == null) {
            return;
        }
        resendQueue.offer(workerTask);
    }
    
    // Send a single pipeline operation to a worker
    public void sendPipelineOperation(Task task, Object currentData) {
        Operation currentOp = task.getCurrentOp();
        if (currentOp == null) {
            // Pipeline complete
            System.out.println("Pipeline complete for task: " + task.getId());
            return;
        }
        if (isReduceOperation(currentOp)) {
            startReduce(task, currentOp, currentData);
            return;
        }
        // Create WorkerTask for this single operation
        WorkerTask workerTask = new WorkerTask(
                task.getId(),
                currentOp,
                currentData,
                task.getCurrentOpIndex(),
                task.getOps().size()
        );
        workerTask.setJarBytes(task.getJarBytes());
        workerTask.setLanguage(currentOp.getLanguage());
        
        sendWorkerTask(workerTask, task);
    }
    
    // Common method to send a WorkerTask to available cluster
    private void sendWorkerTask(WorkerTask wt, Task parentTask) {
        boolean sent = false;
        List<ClusterInfo> clusterList = new ArrayList<>(clusters.values());
        int clusterCount = clusterList.size();

        if (clusterCount == 0) {
            completedResults.put(
                    parentTask.getId(),
                    new TaskResult<>(parentTask.getId(), null, "No clusters registered", "")
            );
            return;
        }

        int attempts = 0;
        while (attempts < clusterCount) {
            ClusterInfo cluster = clusterList.get(currentIndex);
            currentIndex = (currentIndex + 1) % clusterCount;

            try {
                String error = taskSender.sendTask(cluster.getHost(), cluster.getPort(), wt);
                if (error != null && !error.isBlank()) {
                    System.out.println("Failed to send task " + wt.getWorkerTaskId() +
                            " to cluster " + cluster.getId() + ": " + error);
                    attempts++;
                    continue;
                }
                sent = true;
                wt.setClusterInfo(cluster);
                this.healthChecker.register(wt);
                System.out.println("Sent operation " + wt.getOperationIndex() + " of task " + parentTask.getId() + " to cluster " + cluster.getId());
                break;
            } catch (Exception e) {
                sent = false;
                e.printStackTrace();
            }
            attempts++;
        }

        if (!sent) {
            completedResults.put(
                    parentTask.getId(),
                    new TaskResult<>(parentTask.getId(), null, "All clusters failed", "")
            );
        }
    }
    
    // Called when a pipeline operation result is received
    public void onPipelineResult(Task task, TaskResult result) {
        // Mark the operation as complete in tracker for this worker task
        if (result.isSuccess()) {
            pipelineTracker.complete(result.getWorkerTaskId());
        } else {
            pipelineTracker.fail(result.getWorkerTaskId());
        }

        Operation currentOp = task.getCurrentOp();
        if (currentOp != null && isReduceOperation(currentOp)) {
            onReduceResult(task, result);
            return;
        }
        
        if (!result.isSuccess()) {
            // Pipeline failed
            completedResults.put(task.getId(), result);
            // Remove from active tasks
            activeTasks.remove(task.getId());
            pipelineTracker.cleanupTask(task.getId());
            return;
        }
        
        if (task.isLastOp()) {
            // Last operation complete - finalize result
            System.out.println("Pipeline complete for task: " + task.getId());
            completedResults.put(task.getId(), result);
            // Remove from active tasks
            activeTasks.remove(task.getId());
            pipelineTracker.cleanupTask(task.getId());
        } else {
            // More operations to go - advance and send next
            task.advanceOp();
            Object nextData = result.getResult();
            pipelineData.put(task.getId(), nextData);
            sendPipelineOperation(task, nextData);
        }
    }

    private boolean isReduceOperation(Operation op) {
        return op != null && "reduce".equalsIgnoreCase(op.type);
    }

    private String reduceKey(String taskId, int opIndex) {
        return taskId + ":" + opIndex;
    }

    private void startReduce(Task task, Operation op, Object currentData) {
        if (!(currentData instanceof List<?> dataList)) {
            completeReduce(task, currentData);
            return;
        }
        if (dataList.isEmpty()) {
            completeReduce(task, null);
            return;
        }
        if (dataList.size() == 1) {
            completeReduce(task, dataList.get(0));
            return;
        }

        String key = reduceKey(task.getId(), task.getCurrentOpIndex());
        ReduceState state = reduceStates.computeIfAbsent(key, k -> new ReduceState(op));
        dispatchReduceRound(task, state, dataList);
    }

    private void dispatchReduceRound(Task task, ReduceState state, List<?> dataList) {
        ReduceRound round = new ReduceRound();
        state.currentRound = round;

        for (int i = 0; i < dataList.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, dataList.size());
            List<Object> batch = new ArrayList<>(dataList.subList(i, end));

            WorkerTask workerTask = new WorkerTask(
                    task.getId(),
                    state.operation,
                    batch,
                    task.getCurrentOpIndex(),
                    task.getOps().size()
            );
            workerTask.setJarBytes(task.getJarBytes());
            round.register(workerTask.getWorkerTaskId());
            sendWorkerTask(workerTask, task);
        }
    }

    private void onReduceResult(Task task, TaskResult result) {
        String key = reduceKey(task.getId(), task.getCurrentOpIndex());
        ReduceState state = reduceStates.get(key);
        if (state == null) {
            return;
        }

        synchronized (state) {
            if (state.failed) {
                return;
            }
            ReduceRound round = state.currentRound;
            if (round == null || !round.markCompleted(result.getWorkerTaskId())) {
                return;
            }

            if (!result.isSuccess()) {
                state.failed = true;
                completedResults.put(task.getId(), result);
                activeTasks.remove(task.getId());
                pipelineTracker.cleanupTask(task.getId());
                reduceStates.remove(key);
                return;
            }

            if (result.getResult() != null) {
                round.results.add(result.getResult());
            }

            if (round.isComplete()) {
                List<Object> partials = new ArrayList<>(round.results);

                if (partials.isEmpty()) {
                    reduceStates.remove(key);
                    completeReduce(task, null);
                    return;
                }

                if (partials.size() == 1) {
                    reduceStates.remove(key);
                    completeReduce(task, partials.get(0));
                    return;
                }

                dispatchReduceRound(task, state, partials);
            }
        }
    }

    private void completeReduce(Task task, Object reducedValue) {
        if (task.isLastOp()) {
            completedResults.put(task.getId(), new TaskResult<>(task.getId(), reducedValue, null, ""));
            activeTasks.remove(task.getId());
            pipelineTracker.cleanupTask(task.getId());
            return;
        }
        task.advanceOp();
        sendPipelineOperation(task, reducedValue);
    }

    private ClusterInfo pickCluster() {
        if (clusters.isEmpty()) {
            return null;
        }
        return clusters.values().iterator().next();
    }

    public void shutdown() {
        dispatcher.shutdownNow();
        healthChecker.stop();
        network.stop();
    }

    public static void main(String[] args) {
        System.out.println("Initializing manager on port 8085...");
        new TaskScheduler(8085);
    }

    private static class ReduceState {
        private final Operation operation;
        private ReduceRound currentRound;
        private boolean failed;

        private ReduceState(Operation operation) {
            this.operation = operation;
        }
    }

    private static class ReduceRound {
        private final java.util.Set<String> pending = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final java.util.Queue<Object> results = new java.util.concurrent.ConcurrentLinkedQueue<>();

        void register(String workerTaskId) {
            pending.add(workerTaskId);
        }

        boolean markCompleted(String workerTaskId) {
            return pending.remove(workerTaskId);
        }

        boolean isComplete() {
            return pending.isEmpty();
        }
    }
}

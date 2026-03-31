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

public class TaskScheduler {

    private final BlockingQueue<Task> outgoingTasks;
    private final ConcurrentHashMap<String, ClusterInfo> clusters;
    private final ConcurrentHashMap<String, TaskResult<?>> completedResults;
    private final ConcurrentHashMap<String, TaskAggregator> aggregators;
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
        this.clusters = new ConcurrentHashMap<>();
        this.completedResults = new ConcurrentHashMap<>();
        this.aggregators = new ConcurrentHashMap<>();
        this.pipelineData = new ConcurrentHashMap<>();
        this.activeTasks = new ConcurrentHashMap<>();
        this.pipelineTracker = new PipelineTaskTracker();
        this.network = new Network(outgoingTasks, clusters, completedResults, aggregators, managerPort, this);
        this.taskSender = new TaskSender();
        this.dispatcher = Executors.newSingleThreadExecutor();
        this.healthChecker = new HealthChecker(aggregators, pipelineTracker, taskSender);

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
                task = outgoingTasks.take();
                System.out.println("New task received: " + task.getId());
                
                if (task.isPipeline()) {
                    // Handle pipeline task - send first operation
                    System.out.println("Processing pipeline task with " + task.getOps().size() + " operations");
                    // Store task as active for pipeline tracking
                    activeTasks.put(task.getId(), task);
                    sendPipelineOperation(task, task.getInitialData());
                } else {
                    // Handle legacy task - split and send as before
                    List<WorkerTask> workerTasks = splitTask(task, 10);
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
    
    // Send a single pipeline operation to a worker
    public void sendPipelineOperation(Task task, Object currentData) {
        Operation currentOp = task.getCurrentOp();
        if (currentOp == null) {
            // Pipeline complete
            System.out.println("Pipeline complete for task: " + task.getId());
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
                taskSender.sendTask(cluster.getHost(), cluster.getPort(), wt);
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
        // Mark the operation as complete in tracker
        String currentWorkerTaskId = pipelineTracker.getTaskForOperation(task.getId(), task.getCurrentOpIndex());
        if (currentWorkerTaskId != null) {
            pipelineTracker.complete(currentWorkerTaskId);
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

    private ClusterInfo pickCluster() {
        if (clusters.isEmpty()) {
            return null;
        }
        return clusters.values().iterator().next();
    }

    public void shutdown() {
        dispatcher.shutdownNow();
        network.stop();
    }

    public static void main(String[] args) {
        System.out.println("Initializing manager on port 8085...");
        new TaskScheduler(8085);
    }
}

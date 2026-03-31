package cloud.CloudManager;

import cloud.domain.ClusterInfo;
import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskStatus;

import java.util.Map;
import java.util.concurrent.*;

public class HealthChecker {

    private final Map<String, TaskAggregator> aggregators;
    private final PipelineTaskTracker pipelineTracker;

    // workerTaskId → время отправки
    private final ConcurrentHashMap<String, Long> sendTimes = new ConcurrentHashMap<>();

    // workerTaskId → WorkerTask
    private final ConcurrentHashMap<String, WorkerTask> tasks = new ConcurrentHashMap<>();

    private final TaskSender taskSender;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final long GRACE_PERIOD_MS = 2000;

    public HealthChecker(Map<String, TaskAggregator> aggregators,
                         PipelineTaskTracker pipelineTracker,
                         TaskSender taskSender) {
        this.aggregators = aggregators;
        this.pipelineTracker = pipelineTracker;
        this.taskSender = taskSender;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::check, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // вызывается при отправке задачи (как batch, так и pipeline)
    public void register(WorkerTask task) {
        tasks.put(task.getWorkerTaskId(), task);
        sendTimes.put(task.getWorkerTaskId(), System.currentTimeMillis());

        // Register with pipeline tracker if it's a pipeline operation
        if (task.isPipelineOp()) {
            pipelineTracker.register(task);
        }
    }

    private void check() {
        long now = System.currentTimeMillis();

        for (WorkerTask task : tasks.values()) {

            String workerTaskId = task.getWorkerTaskId();
            String taskId = task.getTaskId();

            // Check if already finished
            if (isTaskFinished(task, workerTaskId)) {
                cleanup(workerTaskId);
                continue;
            }

            try {
                WorkerTaskStatus status = checkWorker(task);

                if (status == WorkerTaskStatus.DONE) {
                    // Mark as complete in appropriate tracker
                    if (task.isPipelineOp()) {
                        pipelineTracker.complete(workerTaskId);
                    }
                    cleanup(workerTaskId);
                    continue;
                }

                if (status == WorkerTaskStatus.PENDING) {
                    continue;
                }

                if (status == WorkerTaskStatus.NOT_FOUND) {

                    long sentAt = sendTimes.getOrDefault(workerTaskId, now);

                    if (now - sentAt < GRACE_PERIOD_MS) {
                        // результат может быть "в пути"
                        continue;
                    }

                    // считаем потерянной → retry
                    retry(task);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isTaskFinished(WorkerTask task, String workerTaskId) {
        // For pipeline tasks, check pipeline tracker
        if (task.isPipelineOp()) {
            return pipelineTracker.isFinished(workerTaskId);
        }
        // For batch tasks, check aggregator
        TaskAggregator aggregator = aggregators.get(task.getTaskId());
        return aggregator != null && aggregator.isFinished();
    }

    private WorkerTaskStatus checkWorker(WorkerTask task) {
        // нужно реализовать HTTP GET /health/{workerTaskId}
        try {
            ClusterInfo cluster = task.getClusterInfo();

            return taskSender.checkStatus(
                    cluster.getHost(),
                    cluster.getPort(),
                    task.getWorkerTaskId()
            );

        } catch (Exception e) {
            return WorkerTaskStatus.NOT_FOUND;
        }
    }

    private void retry(WorkerTask task) {
        System.out.println("Retrying task: " + task.getWorkerTaskId());

        try {
            ClusterInfo cluster = task.getClusterInfo();

            taskSender.sendTask(
                    cluster.getHost(),
                    cluster.getPort(),
                    task
            );

            sendTimes.put(task.getWorkerTaskId(), System.currentTimeMillis());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void cleanup(String workerTaskId) {
        WorkerTask task = tasks.remove(workerTaskId);
        sendTimes.remove(workerTaskId);

        // Also cleanup from pipeline tracker if pipeline task
        if (task != null && task.isPipelineOp()) {
            pipelineTracker.cleanup(workerTaskId);
        }
    }
}
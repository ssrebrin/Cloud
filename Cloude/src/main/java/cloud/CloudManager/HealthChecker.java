package cloud.CloudManager;

import cloud.domain.ClusterInfo;
import cloud.domain.WorkerTask;
import cloud.domain.WorkerTaskStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class HealthChecker {

    private final Map<String, TaskAggregator> aggregators;
    private final PipelineTaskTracker pipelineTracker;

    private final ConcurrentHashMap<String, Long> sendTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerTask> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> retryAttempts = new ConcurrentHashMap<>();

    private final TaskSender taskSender;
    private final Consumer<WorkerTask> resendQueueSink;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final long GRACE_PERIOD_MS = 2000;
    private static final int MAX_DIRECT_RETRY_ATTEMPTS = 3;

    public HealthChecker(Map<String, TaskAggregator> aggregators,
                         PipelineTaskTracker pipelineTracker,
                         TaskSender taskSender,
                         Consumer<WorkerTask> resendQueueSink) {
        this.aggregators = aggregators;
        this.pipelineTracker = pipelineTracker;
        this.taskSender = taskSender;
        this.resendQueueSink = resendQueueSink;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::check, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public void register(WorkerTask task) {
        String workerTaskId = task.getWorkerTaskId();
        tasks.put(workerTaskId, task);
        sendTimes.put(workerTaskId, System.currentTimeMillis());
        retryAttempts.put(workerTaskId, 0);

        if (task.isPipelineOp()) {
            pipelineTracker.register(task);
        }
    }

    private void check() {
        long now = System.currentTimeMillis();

        for (WorkerTask task : tasks.values()) {
            String workerTaskId = task.getWorkerTaskId();

            if (isTaskFinished(task, workerTaskId)) {
                cleanup(workerTaskId);
                continue;
            }

            try {
                WorkerTaskStatus status = checkWorker(task);

                if (status == WorkerTaskStatus.DONE) {
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
                        continue;
                    }
                    retry(task);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isTaskFinished(WorkerTask task, String workerTaskId) {
        if (task.isPipelineOp()) {
            return pipelineTracker.isFinished(workerTaskId);
        }
        TaskAggregator aggregator = aggregators.get(task.getTaskId());
        return aggregator != null && aggregator.isFinished();
    }

    private WorkerTaskStatus checkWorker(WorkerTask task) {
        try {
            ClusterInfo cluster = task.getClusterInfo();
            return taskSender.checkStatus(cluster.getHost(), cluster.getPort(), task.getWorkerTaskId());
        } catch (Exception e) {
            return WorkerTaskStatus.NOT_FOUND;
        }
    }

    private void retry(WorkerTask task) {
        String workerTaskId = task.getWorkerTaskId();
        int attempt = retryAttempts.merge(workerTaskId, 1, Integer::sum);
        System.out.println("Retrying task: " + workerTaskId + ", attempt: " + attempt);

        if (attempt > MAX_DIRECT_RETRY_ATTEMPTS) {
            enqueueForResend(task, "Retry limit exceeded");
            return;
        }

        try {
            ClusterInfo cluster = task.getClusterInfo();
            if (cluster == null) {
                enqueueForResend(task, "No assigned cluster");
                return;
            }

            String error = taskSender.sendTask(cluster.getHost(), cluster.getPort(), task);
            if (error != null && !error.isBlank()) {
                if (attempt >= MAX_DIRECT_RETRY_ATTEMPTS) {
                    enqueueForResend(task, "Retry failed: " + error);
                }
                return;
            }

            sendTimes.put(workerTaskId, System.currentTimeMillis());

        } catch (Exception e) {
            if (attempt >= MAX_DIRECT_RETRY_ATTEMPTS) {
                enqueueForResend(task, "Retry exception: " + e.getMessage());
            } else {
                System.out.println("Retry failed for task " + workerTaskId + ": " + e.getMessage());
            }
        }
    }

    private void enqueueForResend(WorkerTask task, String reason) {
        System.out.println("Enqueue for resend: " + task.getWorkerTaskId() + ", reason: " + reason);
        resendQueueSink.accept(task);
        cleanup(task.getWorkerTaskId());
    }

    private void cleanup(String workerTaskId) {
        WorkerTask task = tasks.remove(workerTaskId);
        sendTimes.remove(workerTaskId);
        retryAttempts.remove(workerTaskId);

        if (task != null && task.isPipelineOp()) {
            pipelineTracker.cleanup(workerTaskId);
        }
    }
}

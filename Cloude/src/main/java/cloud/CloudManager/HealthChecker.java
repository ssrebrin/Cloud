package cloud.CloudManager;

import java.util.Map;
import java.util.concurrent.*;

public class HealthChecker {

    private final Map<String, TaskAggregator> aggregators;

    // workerTaskId → время отправки
    private final ConcurrentHashMap<String, Long> sendTimes = new ConcurrentHashMap<>();

    // workerTaskId → WorkerTask
    private final ConcurrentHashMap<String, WorkerTask> tasks = new ConcurrentHashMap<>();

    private final TaskSender taskSender;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final long GRACE_PERIOD_MS = 2000;

    public HealthChecker(Map<String, TaskAggregator> aggregators,
                         TaskSender taskSender) {
        this.aggregators = aggregators;
        this.taskSender = taskSender;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::check, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // 👉 вызывается при отправке задачи
    public void register(WorkerTask task) {
        tasks.put(task.getWorkerTaskId(), task);
        sendTimes.put(task.getWorkerTaskId(), System.currentTimeMillis());
    }

    private void check() {
        long now = System.currentTimeMillis();

        for (WorkerTask task : tasks.values()) {

            String workerTaskId = task.getWorkerTaskId();
            String taskId = task.getTaskId();

            TaskAggregator aggregator = aggregators.get(taskId);

            if (aggregator == null) {
                continue;
            }

            // уже завершена
            if (aggregator.isFinished()) {
                cleanup(workerTaskId);
                continue;
            }

            try {
                WorkerTaskStatus status = checkWorker(task);

                if (status == WorkerTaskStatus.DONE) {
                    cleanup(workerTaskId);
                    continue;
                }

                if (status == WorkerTaskStatus.PENDING) {
                    continue;
                }

                if (status == WorkerTaskStatus.NOT_FOUND) {

                    long sentAt = sendTimes.getOrDefault(workerTaskId, now);

                    if (now - sentAt < GRACE_PERIOD_MS) {
                        // ⏳ результат может быть "в пути"
                        continue;
                    }

                    // ❗ считаем потерянной → retry
                    retry(task);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private WorkerTaskStatus checkWorker(WorkerTask task) {
        // 👉 нужно реализовать HTTP GET /health/{workerTaskId}
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
        tasks.remove(workerTaskId);
        sendTimes.remove(workerTaskId);
    }
}
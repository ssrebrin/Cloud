package cloud.CloudManager;

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
    private final ConcurrentHashMap<String, TaskResult<List<Integer>>> completedResults;
    private final ConcurrentHashMap<String, TaskAggregator> aggregators;
    private final Network network;
    private final TaskSender taskSender;
    private final ExecutorService dispatcher;
    private int currentIndex = 0;

    public TaskScheduler(int managerPort) {
        this.outgoingTasks = new LinkedBlockingQueue<>();
        this.clusters = new ConcurrentHashMap<>();
        this.completedResults = new ConcurrentHashMap<>();
        this.aggregators = new ConcurrentHashMap<>();
        this.network = new Network(outgoingTasks, clusters, completedResults, aggregators, managerPort);
        this.taskSender = new TaskSender();
        this.dispatcher = Executors.newSingleThreadExecutor();

        new Thread(network).start();
        dispatcher.submit(this::dispatchLoop);
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

    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            Task task = null;

            try {
                task = outgoingTasks.take();
                System.out.println("New task");
                List<WorkerTask> workerTasks = splitTask(task, 10);

                TaskAggregator aggregator = new TaskAggregator(task.getId(), workerTasks);

                aggregators.put(task.getId(), aggregator);

                for (WorkerTask wt : workerTasks) {

                    boolean sent = false;

                    List<ClusterInfo> clusterList = new ArrayList<>(clusters.values());

                    int clusterCount = clusterList.size();

                    if (clusterCount == 0) {
                        completedResults.put(
                                task.getId(),
                                new TaskResult<>(task.getId(), null, "No clusters registered", "")
                        );
                        break;
                    }

                    int attempts = 0;

                    while (attempts < clusterCount) {
                        ClusterInfo cluster = clusterList.get(currentIndex);

                        currentIndex = (currentIndex + 1) % clusterCount;

                        try {
                            taskSender.sendTask(
                                    cluster.getHost(),
                                    cluster.getPort(),
                                    wt
                            );
                            sent = true;
                            break;

                        } catch (Exception e) {
                            sent = false;
                            e.printStackTrace();
                        }

                        attempts++;
                    }

                    if (!sent) {
                        completedResults.put(
                                task.getId(),
                                new TaskResult<>(task.getId(), null, "All clusters failed", "")
                        );
                        break;
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

package cloud.CloudManager;

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
    private final Network network;
    private final TaskSender taskSender;
    private final ExecutorService dispatcher;

    public TaskScheduler(int managerPort) {
        this.outgoingTasks = new LinkedBlockingQueue<>();
        this.clusters = new ConcurrentHashMap<>();
        this.completedResults = new ConcurrentHashMap<>();
        this.network = new Network(outgoingTasks, clusters, completedResults, managerPort);
        this.taskSender = new TaskSender();
        this.dispatcher = Executors.newSingleThreadExecutor();

        new Thread(network).start();
        dispatcher.submit(this::dispatchLoop);
    }

    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            Task task = null;
            try {
                task = outgoingTasks.take();
                ClusterInfo cluster = pickCluster();

                if (cluster == null) {
                    completedResults.put(task.getId(), new TaskResult<>(task.getId(), "No clusters registered"));
                    continue;
                }

                TaskResult<List<Integer>> result = taskSender.sendTask(cluster.getHost(), cluster.getPort(), task);
                completedResults.put(task.getId(), result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (task != null) {
                    completedResults.put(task.getId(), new TaskResult<>(task.getId(), e.getMessage()));
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
    }

    public static void main(String[] args) {
        System.out.println("Initializing manager on port 8085...");
        new TaskScheduler(8085);
    }
}

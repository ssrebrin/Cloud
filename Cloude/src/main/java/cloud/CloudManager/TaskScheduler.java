package cloud.CloudManager;

import cloud.cloud.RemoteFunction;
import cloud.CloudManager.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class TaskScheduler {
    private final BlockingQueue<Task> outgoingTasks;
    private final BlockingQueue<cloud.CloudManager.TaskResult> incomingResults;
    private final cloud.CloudManager.Network network;

    public TaskScheduler(BlockingQueue<Task> outgoingTasks, BlockingQueue<cloud.CloudManager.TaskResult> incomingResults, cloud.CloudManager.Network network) {
        this.outgoingTasks = outgoingTasks;
        this.incomingResults = incomingResults;
        this.network = network;
        new Thread(network).start();
    }

    public void submit(Task task) throws InterruptedException {
        outgoingTasks.put(task);
    }

    public cloud.CloudManager.TaskResult getResult() throws InterruptedException {
        return incomingResults.take();
    }
    public static void main(String[] args) {
        final BlockingQueue<Task> outgoingTasks = new LinkedBlockingQueue<>();
        final BlockingQueue<cloud.CloudManager.TaskResult> incomingResults = new LinkedBlockingQueue<>();
        final ConcurrentHashMap<String, cloud.CloudManager.ClusterInfo> clusters = new ConcurrentHashMap<>();
        System.out.println("Initializing server...");

        Network network = new Network(outgoingTasks, incomingResults, clusters, 8085);
        network.run();

        /*Thread networkThread = new Thread(network);
        networkThread.start();*/

        System.out.println("Waiting for clusters to register...");

        TaskScheduler ts = new TaskScheduler(outgoingTasks, incomingResults, network);

        Executors.newSingleThreadScheduledExecutor().schedule(() -> {

            if (clusters.isEmpty()) {
                System.out.println("No clusters registered yet.");
                return;
            }

            ClusterInfo cluster = clusters.values().iterator().next();

            Task task = new Task<>(
                    (RemoteFunction<Object, Object>) x -> {
                        System.out.println("Executing task: x=" + x);
                        return ((Integer) x) * 2;
                    },
                    10
            );

            try {
                TaskSender sender = new TaskSender();
                sender.sendTask(cluster.getHost(), cluster.getPort(), task);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }, 5, TimeUnit.SECONDS);


    }
}


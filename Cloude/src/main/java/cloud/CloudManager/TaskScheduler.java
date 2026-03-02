package cloud.CloudManager;

import cloud.Task;
import cloud.TaskResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskScheduler {
    private final BlockingQueue<Task> outgoingTasks;
    private final BlockingQueue<TaskResult> incomingResults;
    private final Network network;

    public TaskScheduler(BlockingQueue<Task> outgoingTasks, BlockingQueue<TaskResult> incomingResults, Network network) {
        this.outgoingTasks = outgoingTasks;
        this.incomingResults = incomingResults;
        this.network = network;
        new Thread(network).start();
    }

    public void submit(Task task) throws InterruptedException {
        outgoingTasks.put(task); // ставим задачу в канал
    }

    public TaskResult getResult() throws InterruptedException {
        return incomingResults.take();
    }
    static void main(String[] args) {
        final BlockingQueue<Task> outgoingTasks = new LinkedBlockingQueue<>();
        final BlockingQueue<TaskResult> incomingResults = new LinkedBlockingQueue<>();
        final Map<String, ClusterInfo> clusters = new HashMap<>();
        TaskScheduler ts = new TaskScheduler(outgoingTasks, incomingResults, new Network(outgoingTasks, incomingResults, clusters));


    }
}

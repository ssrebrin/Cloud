package cloud.Cluster;
import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public <T extends Serializable, R> void execute(Task<T, R> task,
                                                    cloud.Cluster.ResultCallback<R> callback) {

        executor.submit(() -> {
            try {
                R result = task.getFunction().apply(task.getArgument());

                TaskResult<R> taskResult =
                        new TaskResult<>(task.getId(), result);

                callback.onResult(taskResult);

            } catch (Exception e) {
                TaskResult<R> taskResult =
                        new TaskResult<>(task.getId(), e.getMessage());

                callback.onResult(taskResult);
            }
        });
    }

    public void shutdown() {
        executor.shutdown();
    }
}
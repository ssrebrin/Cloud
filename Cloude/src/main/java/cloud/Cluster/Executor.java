package cloud.Cluster;

import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;

import javax.xml.transform.Result;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Executor {
    private final BlockingQueue<Task> Tasks;
    private final BlockingQueue<Result> Results;

    public Executor(BlockingQueue<Task> tasks, BlockingQueue<Result> results) {
        Tasks = tasks;
        Results = results;
    }

    public TaskResult<List<Integer>> execute(Task task) {
        try {
            List<Integer> values = task.getValues() == null ? List.of() : task.getValues();
            List<Integer> result = values.stream()
                    .map(value -> applyStub(task.getFunctionStub(), value))
                    .toList();

            return new TaskResult<>(task.getId(), result, null);
        } catch (Exception e) {
            return new TaskResult<>(task.getId(), null, e.getMessage());
        }
    }

    private int applyStub(String functionStub, int value) {
        // Stub execution for now: placeholder function is accepted but ignored.
        return value;
    }
}

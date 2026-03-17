package cloud.Cluster;

import cloud.CloudManager.Task;
import cloud.CloudManager.TaskResult;

import java.util.List;

public class Worker {

    public TaskResult<List<Integer>> execute(Task task) {
        try {
            List<Integer> values = task.getValues() == null ? List.of() : task.getValues();
            List<Integer> result = values.stream()
                    .map(value -> applyStub(task.getFunctionStub(), value))
                    .toList();

            return new TaskResult<>(task.getId(), result);
        } catch (Exception e) {
            return new TaskResult<>(task.getId(), e.getMessage());
        }
    }

    private int applyStub(String functionStub, int value) {
        // Stub execution for now: placeholder function is accepted but ignored.
        return value;
    }
}

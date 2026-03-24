package cloud.CloudManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WorkerTask implements Serializable {

    private String taskId;
    private String workerTaskId;
    private String functionStub;
    private String serializedFunction;
    private String jarBytes;
    private List<Integer> values;

    public WorkerTask() {}

    public WorkerTask(String taskId, String functionStub, List<Integer> values) {
        this.taskId = taskId;
        this.workerTaskId = UUID.randomUUID().toString();
        this.functionStub = functionStub;
        this.values = new ArrayList<>(values);
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getWorkerTaskId() {
        return workerTaskId;
    }

    public void setWorkerTaskId(String workerTaskId) {
        this.workerTaskId = workerTaskId;
    }

    public String getFunctionStub() {
        return functionStub;
    }

    public void setFunctionStub(String functionStub) {
        this.functionStub = functionStub;
    }

    public String getSerializedFunction() {
        return serializedFunction;
    }

    public void setSerializedFunction(String serializedFunction) {
        this.serializedFunction = serializedFunction;
    }

    public String getJarBytes() {
        return jarBytes;
    }

    public void setJarBytes(String jarBytes) {
        this.jarBytes = jarBytes;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }
}
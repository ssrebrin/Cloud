package cloud.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkerTask implements Serializable {

    private String taskId;
    private String workerTaskId;
    private String functionStub;
    private String serializedFunction;
    private String jarBytes;
    private List<Integer> values;
    private ClusterInfo clusterInfo;
    
    // Pipeline fields - single operation execution
    private Operation operation;
    private Object currentData;
    private int operationIndex;
    private int totalOperations;
    private boolean isPipelineOp;

    public WorkerTask() {}

    // Legacy constructor
    public WorkerTask(String taskId, String functionStub, List<Integer> values) {
        this.taskId = taskId;
        this.workerTaskId = UUID.randomUUID().toString();
        this.functionStub = functionStub;
        this.values = new ArrayList<>(values);
        this.isPipelineOp = false;
    }
    
    // Constructor for pipeline operation
    public WorkerTask(String taskId, Operation operation, Object currentData, int operationIndex, int totalOperations) {
        this.taskId = taskId;
        this.workerTaskId = UUID.randomUUID().toString();
        this.operation = operation;
        this.currentData = currentData;
        this.operationIndex = operationIndex;
        this.totalOperations = totalOperations;
        this.isPipelineOp = true;
        this.values = new ArrayList<>();
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

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }
    
    // Pipeline getters/setters
    public Operation getOperation() {
        return operation;
    }
    
    public void setOperation(Operation operation) {
        this.operation = operation;
    }
    
    public Object getCurrentData() {
        return currentData;
    }
    
    public void setCurrentData(Object currentData) {
        this.currentData = currentData;
    }
    
    public int getOperationIndex() {
        return operationIndex;
    }
    
    public void setOperationIndex(int operationIndex) {
        this.operationIndex = operationIndex;
    }
    
    public int getTotalOperations() {
        return totalOperations;
    }
    
    public void setTotalOperations(int totalOperations) {
        this.totalOperations = totalOperations;
    }
    
    public boolean isPipelineOp() {
        return isPipelineOp;
    }
    
    public void setPipelineOp(boolean pipelineOp) {
        isPipelineOp = pipelineOp;
    }
    
    public boolean isLastOperation() {
        return isPipelineOp && operationIndex == totalOperations - 1;
    }
}

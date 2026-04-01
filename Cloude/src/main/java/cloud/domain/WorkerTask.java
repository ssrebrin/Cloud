package cloud.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnore;

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
    @JsonIgnore
    private ClusterInfo clusterInfo;
    private String language;
    
    // Pipeline fields - single operation execution
    private Operation operation;
    private Object currentData;
    private int operationIndex;
    private int totalOperations;
    private boolean isPipelineOp;

    public WorkerTask() {
        this.language = Task.LANGUAGE_JAVA;
    }

    // Legacy constructor
    public WorkerTask(String taskId, String functionStub, List<Integer> values) {
        this.taskId = taskId;
        this.workerTaskId = UUID.randomUUID().toString();
        this.functionStub = functionStub;
        this.values = new ArrayList<>(values);
        this.isPipelineOp = false;
        this.language = Task.LANGUAGE_JAVA;
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
        this.language = operation == null ? Task.LANGUAGE_JAVA : operation.getLanguage();
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

    @JsonIgnore
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @JsonIgnore
    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public String getLanguage() {
        if (language == null || language.isBlank()) {
            return Task.LANGUAGE_JAVA;
        }
        return language;
    }

    public void setLanguage(String language) {
        if (language == null || language.isBlank()) {
            this.language = Task.LANGUAGE_JAVA;
            return;
        }
        this.language = language;
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

package cloud.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Task implements Serializable {

    public static final String LANGUAGE_JAVA = "java";
    public static final String LANGUAGE_CLOJURE = "clojure";

    private String id;
    private String functionStub;
    private String serializedFunction;
    private String jarBytes;
    private List<Integer> values;
    private String callback;
    private String language;
    
    // Pipeline fields
    private List<Operation> ops;
    private Object initialData;
    private int currentOpIndex;
    private boolean isPipeline;

    public Task() {
        // Default constructor for Jackson
        this.ops = new ArrayList<>();
        this.currentOpIndex = 0;
        this.isPipeline = false;
        this.language = LANGUAGE_JAVA;
    }

    // Legacy constructor for single function task
    public Task(String functionStub, String serializedFunction, String jarBytes, List<Integer> values, String callback) {
        this(functionStub, serializedFunction, jarBytes, values, callback, LANGUAGE_JAVA);
    }

    public Task(String functionStub, String serializedFunction, String jarBytes, List<Integer> values, String callback, String language) {
        this.id = UUID.randomUUID().toString();
        this.functionStub = functionStub;
        this.serializedFunction = serializedFunction;
        this.jarBytes = jarBytes;
        this.values = new ArrayList<>(values);
        this.callback = callback;
        this.language = normalizeLanguage(language);
        this.ops = new ArrayList<>();
        this.currentOpIndex = 0;
        this.isPipeline = false;
    }
    
    // Constructor for pipeline task
    public Task(List<Operation> ops, Object initialData, String jarBytes, String callback) {
        this(ops, initialData, jarBytes, callback, LANGUAGE_JAVA);
    }

    public Task(List<Operation> ops, Object initialData, String jarBytes, String callback, String language) {
        this.id = UUID.randomUUID().toString();
        this.ops = new ArrayList<>(ops);
        this.initialData = initialData;
        this.jarBytes = jarBytes;
        this.callback = callback;
        this.language = normalizeLanguage(language);
        this.currentOpIndex = 0;
        this.isPipeline = true;
        this.values = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getLanguage() {
        return normalizeLanguage(language);
    }

    public void setLanguage(String language) {
        this.language = normalizeLanguage(language);
    }
    
    // Pipeline getters/setters
    public List<Operation> getOps() {
        return ops;
    }
    
    public void setOps(List<Operation> ops) {
        this.ops = ops;
    }
    
    public Object getInitialData() {
        return initialData;
    }
    
    public void setInitialData(Object initialData) {
        this.initialData = initialData;
    }
    
    public int getCurrentOpIndex() {
        return currentOpIndex;
    }
    
    public void setCurrentOpIndex(int currentOpIndex) {
        this.currentOpIndex = currentOpIndex;
    }
    
    public boolean isPipeline() {
        return isPipeline;
    }
    
    public void setPipeline(boolean pipeline) {
        isPipeline = pipeline;
    }
    
    // Helper methods for pipeline execution
    public boolean hasMoreOps() {
        return isPipeline && currentOpIndex < ops.size();
    }
    
    public Operation getCurrentOp() {
        if (!isPipeline || currentOpIndex >= ops.size()) {
            return null;
        }
        return ops.get(currentOpIndex);
    }
    
    public void advanceOp() {
        currentOpIndex++;
    }
    
    public boolean isLastOp() {
        return isPipeline && currentOpIndex == ops.size() - 1;
    }

    private String normalizeLanguage(String language) {
        if (language == null || language.isBlank()) {
            return LANGUAGE_JAVA;
        }
        return language;
    }
}

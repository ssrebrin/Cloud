package cloud.CloudManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Task implements Serializable {

    private String id;
    private String functionStub;
    private List<Integer> values;
    private String callback;

    public Task() {
        // Default constructor for Jackson
    }

    public Task(String functionStub, List<Integer> values, String callback) {
        this.id = UUID.randomUUID().toString();
        this.functionStub = functionStub;
        this.values = new ArrayList<>(values);
        this.callback = callback;
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

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }
}

package cloud.domain;

import java.io.Serializable;

public class Operation implements Serializable {
    public String type;
    public byte[] function;

    // Default constructor for Jackson
    public Operation() {
    }

    public Operation(String type, byte[] function) {
        this.type = type;
        this.function = function;
    }
}

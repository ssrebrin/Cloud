package cloud.demo;

import java.io.Serializable;

public class ComplexTask implements Serializable {
    private final String name;
    private final int multiplier;

    public ComplexTask(String name, int multiplier) {
        this.name = name;
        this.multiplier = multiplier;
    }

    public String getName() {
        return name;
    }

    public int getMultiplier() {
        return multiplier;
    }
}

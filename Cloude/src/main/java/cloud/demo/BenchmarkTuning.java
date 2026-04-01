package cloud.demo;

import java.io.Serializable;

public class BenchmarkTuning implements Serializable {
    private final int multiplier;
    private final int salt;
    private final int iterations;
    private final int filterModulo;
    private final int filterRemainder;

    public BenchmarkTuning(int multiplier, int salt, int iterations, int filterModulo, int filterRemainder) {
        this.multiplier = multiplier;
        this.salt = salt;
        this.iterations = iterations;
        this.filterModulo = filterModulo;
        this.filterRemainder = filterRemainder;
    }

    public int getMultiplier() {
        return multiplier;
    }

    public int getSalt() {
        return salt;
    }

    public int getIterations() {
        return iterations;
    }

    public int getFilterModulo() {
        return filterModulo;
    }

    public int getFilterRemainder() {
        return filterRemainder;
    }
}

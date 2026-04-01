package cloud.demo;

public class SignalMath {
    private static final int MOD = 1_000_003;

    private SignalMath() {
    }

    public static boolean shouldKeep(int value, BenchmarkTuning tuning) {
        return Math.floorMod(value + tuning.getSalt(), tuning.getFilterModulo()) == tuning.getFilterRemainder();
    }

    public static int transform(int value, BenchmarkTuning tuning) {
        int acc = value ^ tuning.getSalt();
        for (int i = 0; i < tuning.getIterations(); i++) {
            acc = Integer.rotateLeft(acc + (i * 31), 3) ^ (acc >>> 2);
            acc += tuning.getMultiplier() * (i + 1);
        }
        return Math.floorMod(acc, MOD);
    }

    public static int combine(int left, int right) {
        return Math.floorMod(left + right, MOD);
    }
}

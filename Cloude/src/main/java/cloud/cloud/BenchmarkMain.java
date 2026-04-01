package cloud.cloud;

import cloud.demo.BenchmarkTuning;
import cloud.demo.SignalMath;
import cloud.serialization.CloudClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntSupplier;

public class BenchmarkMain {

    private static final int DEFAULT_SIZE = 250_000;

    public static void main(String[] args) throws IOException, InterruptedException {
        String managerUrl = args.length > 0 ? args[0] : "http://localhost:8085";
        int dataSize = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_SIZE;

        BenchmarkTuning tuning = new BenchmarkTuning(
                7,
                131,
                35,
                5,
                2
        );

        int[] data = generateData(dataSize);
        System.out.println("Benchmark dataset size: " + data.length);
        System.out.println("Manager URL: " + managerUrl);
        System.out.println("Running sequential / parallel / cloud...");

        // Warm-up to reduce one-time class loading noise before measurement.
        runSequential(Arrays.copyOf(data, Math.min(10_000, data.length)), tuning);
        runParallel(Arrays.copyOf(data, Math.min(10_000, data.length)), tuning);

        BenchmarkResult sequential = measure("sequential", () -> runSequential(data, tuning));
        BenchmarkResult parallel = measure("parallel", () -> runParallel(data, tuning));
        BenchmarkResult cloud = measure("cloud", () -> runCloud(managerUrl, data, tuning));

        ensureSameResult(sequential, parallel, cloud);

        System.out.println();
        System.out.println("Checksums: " + sequential.checksum);
        System.out.println("Sequential: " + sequential.millis + " ms");
        System.out.println("Parallel:   " + parallel.millis + " ms (x" + formatSpeedup(sequential, parallel) + ")");
        System.out.println("Cloud:      " + cloud.millis + " ms (x" + formatSpeedup(sequential, cloud) + ")");
    }

    private static int[] generateData(int size) {
        int[] data = new int[size];
        int x = 17;
        for (int i = 0; i < size; i++) {
            x = x * 1103515245 + 12345;
            data[i] = x & 0x7fffffff;
        }
        return data;
    }

    private static int runSequential(int[] data, BenchmarkTuning tuning) {
        return Arrays.stream(data)
                .filter(x -> SignalMath.shouldKeep(x, tuning))
                .map(x -> SignalMath.transform(x, tuning))
                .reduce(SignalMath::combine)
                .orElse(0);
    }

    private static int runParallel(int[] data, BenchmarkTuning tuning) {
        return Arrays.stream(data)
                .parallel()
                .filter(x -> SignalMath.shouldKeep(x, tuning))
                .map(x -> SignalMath.transform(x, tuning))
                .reduce(SignalMath::combine)
                .orElse(0);
    }

    private static int runCloud(String managerUrl, int[] data, BenchmarkTuning tuning) {
        try {
            List<Integer> result = CloudStream.<Integer>connect(managerUrl)
                    .stream(data)
                    .filter(x -> SignalMath.shouldKeep(x, tuning))
                    .map(x -> SignalMath.transform(x, tuning))
                    .reduce(SignalMath::combine)
                    .execute(SignalMath.class, BenchmarkTuning.class, BenchmarkMain.class, BenchmarkResult.class);

            return result.isEmpty() ? 0 : result.get(0);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static BenchmarkResult measure(String label, IntSupplier fn) {
        long start = System.nanoTime();
        int checksum = fn.getAsInt();
        long elapsed = System.nanoTime() - start;
        long millis = elapsed / 1_000_000;
        System.out.println(label + " done in " + millis + " ms");
        return new BenchmarkResult(label, checksum, millis);
    }

    private static void ensureSameResult(BenchmarkResult sequential, BenchmarkResult parallel, BenchmarkResult cloud) {
        if (sequential.checksum != parallel.checksum || sequential.checksum != cloud.checksum) {
            throw new IllegalStateException(
                    "Result mismatch: sequential=" + sequential.checksum +
                            ", parallel=" + parallel.checksum +
                            ", cloud=" + cloud.checksum
            );
        }
    }

    private static String formatSpeedup(BenchmarkResult baseline, BenchmarkResult candidate) {
        if (candidate.millis == 0) {
            return "inf";
        }
        double speedup = (double) baseline.millis / (double) candidate.millis;
        return String.format("%.2f", speedup);
    }

    private record BenchmarkResult(String label, int checksum, long millis) {
    }
}

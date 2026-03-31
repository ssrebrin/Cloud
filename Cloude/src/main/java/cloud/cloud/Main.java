package cloud.cloud;

import cloud.demo.Calculator;
import cloud.demo.ComplexTask;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        ComplexTask taskConfig = new ComplexTask("Fibonacci Task", 2);

        List<Integer> result = CloudStream.<Integer>connect("http://localhost:8085")
                .stream(new int[]{5, 6, 7, 8})
                .filter(x -> x % 2 == 0)
                .map(x -> x * taskConfig.getMultiplier())
                .reduce((x, y) -> x * y)
                .execute(Calculator.class, ComplexTask.class);

        System.out.println("[java][pipeline] result: " + result);
    }
}

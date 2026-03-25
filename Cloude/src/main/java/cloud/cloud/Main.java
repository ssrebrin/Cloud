package cloud.cloud;

import cloud.demo.Calculator;
import cloud.demo.ComplexTask;
import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Создаем объект с данными, который будет захвачен лямбдой (проверка Kryo)
        ComplexTask taskConfig = new ComplexTask("Fibonacci Task", 2);
        
        // Лямбда использует внешний статический класс Calculator (проверка JAR/ClassLoader)
        // и захваченный объект taskConfig (проверка Kryo/LambdaWrapper)
        RemoteFunction<Integer, Integer> f = x -> {
            int fib = Calculator.fibonacci(x);
            System.out.println("Processing " + taskConfig.getName() + " for " + x);
            return fib * taskConfig.getMultiplier();
        };

        Cloud cloud = Cloud.connect("http://localhost:8085");

        // Мы должны указать все классы, которые нужны воркеру, 
        // но которых у него нет в classpath (Calculator и ComplexTask).
        // В реальной системе это можно автоматизировать через анализ байткода.
        List<Integer> result = cloud.execute(f, new int[]{5, 6, 7, 8}, 
                Calculator.class, ComplexTask.class);
        
        System.out.println("Result (Fibonacci * 2): " + result);
    }
}

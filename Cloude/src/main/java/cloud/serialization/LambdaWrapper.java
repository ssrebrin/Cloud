package cloud.serialization;

import java.io.Serializable;

/**
 * Обертка для лямбда-выражений, которая заставляет их проходить через стандартную Java-сериализацию.
 */
public class LambdaWrapper implements Serializable {
    private final Object lambda;

    public LambdaWrapper(Object lambda) {
        this.lambda = lambda;
    }

    public Object getLambda() {
        return lambda;
    }
}

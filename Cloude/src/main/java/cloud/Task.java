package cloud;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Task<T extends Serializable, R> implements Serializable {

    private final String id;
    private final RemoteFunction<T, R> function;
    private final T argument;

    // не сериализуем — используется только внутри менеджера
    private transient CompletableFuture<R> future;

    public Task(RemoteFunction<T, R> function, T argument) {
        this.id = UUID.randomUUID().toString();
        this.function = function;
        this.argument = argument;
        this.future = new CompletableFuture<>();
    }

    public String getId() {
        return id;
    }

    public RemoteFunction<T, R> getFunction() {
        return function;
    }

    public T getArgument() {
        return argument;
    }

    public CompletableFuture<R> getFuture() {
        return future;
    }

    public void setFuture(CompletableFuture<R> future) {
        this.future = future;
    }
}
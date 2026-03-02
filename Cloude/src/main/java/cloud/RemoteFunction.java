package cloud;

@FunctionalInterface
public interface RemoteFunction<T,R> extends java.io.Serializable {
    R apply(T t);
}
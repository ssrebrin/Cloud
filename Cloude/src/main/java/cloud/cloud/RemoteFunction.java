package cloud.cloud;

@SuppressWarnings("unchecked")
@FunctionalInterface
public interface RemoteFunction<T,R> extends java.io.Serializable {
    R apply(T t);
}


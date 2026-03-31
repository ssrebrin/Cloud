package cloud.domain;

import java.io.Serializable;

@SuppressWarnings("unchecked")
@FunctionalInterface
public interface RemoteFunction<T extends Serializable,R> extends Serializable {
    R apply(T t);
}

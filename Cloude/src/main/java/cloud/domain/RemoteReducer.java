package cloud.domain;

import java.io.Serializable;

@FunctionalInterface
public interface RemoteReducer<T extends Serializable> extends Serializable {
    T apply(T left, T right);
}

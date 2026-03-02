package cloud.Cluster;

import cloud.TaskResult;

public interface ResultCallback<R> {
    void onResult(TaskResult<R> result);
}

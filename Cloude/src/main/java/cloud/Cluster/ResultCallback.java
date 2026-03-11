package cloud.Cluster;

import cloud.CloudManager.TaskResult;

public interface ResultCallback<R> {
    void onResult(TaskResult<R> result);
}

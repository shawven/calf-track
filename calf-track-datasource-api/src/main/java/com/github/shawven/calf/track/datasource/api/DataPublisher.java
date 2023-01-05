package com.github.shawven.calf.track.datasource.api;

import com.github.shawven.calf.track.datasource.api.domain.BaseRows;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface DataPublisher {

    void publish(BaseRows data);

    default void destroy() {

    }

    default long getPublishCount() {
        return 0L;
    }

    default long publishCountSinceLastTime() {
        return 0L;
    }
}

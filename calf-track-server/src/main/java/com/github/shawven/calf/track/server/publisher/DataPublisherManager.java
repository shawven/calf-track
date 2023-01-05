package com.github.shawven.calf.track.server.publisher;

import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xw
 * @date 2023-01-05
 */
public class DataPublisherManager implements DataPublisher {

    private final Map<String, DataPublisher> dataPublisherMap;

    private final AtomicLong publishCount = new AtomicLong(0);

    private long lastPublishCount = 0;

    public DataPublisherManager(Map<String, DataPublisher> dataPublisherMap) {
        this.dataPublisherMap = dataPublisherMap;
    }

    public void publish(BaseRows data) {
        String destQueue = data.getDestQueue();
        DataPublisher publisher = selectDataPublisher(destQueue);
        if (publisher == null) {
            throw new RuntimeException("The queue publisher was not found: " + destQueue);
        }
        publisher.publish(data);

        publishCount.incrementAndGet();
    }

    @Override
    public long getPublishCount() {
        return publishCount.get();
    }

    @Override
    public long publishCountSinceLastTime() {
        long total = publishCount.get();
        long durationTotal =  total - lastPublishCount;

        lastPublishCount = total;

        return durationTotal;
    }

    public DataPublisher selectDataPublisher(String queueType) {
        return dataPublisherMap.get(queueType.toLowerCase() + "DataPublisher");
    }
}

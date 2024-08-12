package com.github.shawven.calf.track.client;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface DataConsumer {

    String queueType();

    void startConsumers(String clientId, List<DataSubscribeHandler> handlers);

    void stopConsumers(String clientId);
}

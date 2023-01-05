package com.github.shawven.calf.track.client;

import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface DataConsumer {

    void startConsumers(String clientId, Map<String, DataSubscribeHandler> handlerMap);

    void stopConsumers(String clientId);
}

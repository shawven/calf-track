package com.github.shawven.calf.track.client;


import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xw
 * @date 2023-01-03
 */
@Setter
@Accessors(chain = true)
public class DataSubscribeRegistry implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(DataSubscribeRegistry.class);

    private final static Map<String, DataSubscribeHandler> HANDLER_MAP = new ConcurrentHashMap<>();

    private String clientId;

    private String serverUrl;

    private DataConsumer dataConsumer;

    private volatile boolean running;

    public DataSubscribeRegistry registerHandler(DataSubscribeHandler handler) {
        logger.info("registerHandler key: {}, actions:{}", handler.key(), Arrays.toString(handler.actions()));
        HANDLER_MAP.put(handler.key(), handler);
        return this;
    }

    /**
     * 同步到服务器
     *
     * @return
     */
    public DataSubscribeRegistry syncToServer() {
        logger.info("start syncToServer url: {}", serverUrl);
        try {
            if (serverUrl != null && !HANDLER_MAP.isEmpty()) {

            }
        } catch (Exception e) {
            logger.error("syncToServer error: " + e.getMessage(), e);
        }
        return this;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 执行监听
     */
    public void start() {
        if (isRunning()) {
            return;
        }
        logger.info("startConsumers");
        running = true;
        dataConsumer.startConsumers(clientId, HANDLER_MAP);
        logger.info("successfully startConsumers");
    }

    @Override
    public void stop() {
        logger.info("stopConsumers");
        running = false;
        dataConsumer.stopConsumers(clientId);
        logger.info("successfully stopConsumers");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

}

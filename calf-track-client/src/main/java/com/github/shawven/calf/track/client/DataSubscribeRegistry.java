package com.github.shawven.calf.track.client;


import com.github.shawven.calf.track.client.annotation.DataListenerAnnotationBeanPostProcessor;
import com.github.shawven.calf.track.common.Const;
import com.google.gson.Gson;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    /**
     * 注册处理器
     *
     * @see DataListenerAnnotationBeanPostProcessor
     * @param handler
     */
    public void registerHandler(DataSubscribeHandler handler) {
        String key = Const.uniqueKey(handler.namespace(), handler.dataSource(), handler.database(), handler.table());
        logger.info("registerHandler key: {}, actions:{}", key, Arrays.toString(handler.actions()));
        HANDLER_MAP.put(key, handler);
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
//                for (DataSubscribeHandler handler : HANDLER_MAP.values()) {
//                    registerToServer(handler);
//                }
            }
        } catch (Exception e) {
            logger.error("syncToServer error: " + e.getMessage(), e);
        }
        return this;
    }

    private void registerToServer(DataSubscribeHandler handler) throws IOException {
        HttpPost request = new HttpPost(serverUrl + "/client/addAll?namespace=" + handler.namespace());
        request.setEntity(new StringEntity(convertBody(handler), ContentType.APPLICATION_JSON));

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(request)) {
            EntityUtils.consume(response.getEntity());
        }
    }

    private String convertBody(DataSubscribeHandler handler)  {
        ArrayList<Object> objects = new ArrayList<>();
        Map<String, Object> client = new HashMap<>();
        client.put("name", clientId);
        client.put("queueType", dataConsumer.queueType());
        client.put("dsName", handler.dataSource());
        client.put("dbName", handler.database());
        client.put("tableName", handler.table());
        client.put("eventActions", Arrays.stream(handler.actions()).map(Enum::name).collect(Collectors.toList()));
        objects.add(client);

        return new Gson().toJson(objects);
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
        dataConsumer.startConsumers(clientId, new ArrayList<>(HANDLER_MAP.values()));
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

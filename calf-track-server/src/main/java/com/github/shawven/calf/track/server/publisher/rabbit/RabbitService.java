package com.github.shawven.calf.track.server.publisher.rabbit;

import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.Collections;
import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
public class RabbitService {

    private RabbitTemplate rabbitTemplate;

    private Client rabbitHttpClient;

    public RabbitService(RabbitTemplate rabbitTemplate, Client rabbitHttpClient) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitHttpClient = rabbitHttpClient;
    }

    public QueueInfo getQueue(String name) {
        String virtualHost = rabbitTemplate.getConnectionFactory().getVirtualHost();
        return rabbitHttpClient.getQueue(virtualHost, name);
    }

    public List<BaseRows> getMessageList(String clientId, long count) {

        return Collections.emptyList();
    }
}

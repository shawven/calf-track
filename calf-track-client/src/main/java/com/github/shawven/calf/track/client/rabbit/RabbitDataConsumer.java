package com.github.shawven.calf.track.client.rabbit;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.client.DataConsumer;
import com.github.shawven.calf.track.client.DataSubscribeHandler;
import com.google.gson.Gson;
import com.rabbitmq.client.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @auther: chenjh
 * @time: 2018/11/20 10:18
 * @description
 */
public class RabbitDataConsumer implements DataConsumer {

    private final Logger logger = LoggerFactory.getLogger(RabbitDataConsumer.class);

    private final RabbitTemplate rabbitTemplate;

    private final List<SimpleConsumer> consumers = new ArrayList<>();

    public RabbitDataConsumer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void startConsumers(String clientId, Map<String, DataSubscribeHandler> handlerMap) {
        for (Map.Entry<String, DataSubscribeHandler> entry : handlerMap.entrySet()) {
            try {
                consume(clientId, entry.getKey(), entry.getValue());
            } catch (IOException e) {
                logger.error("subscribe failed :" + e.getMessage(), e);
            }
        }
    }

    private void consume(String clientId, String routingKey, DataSubscribeHandler handler) throws IOException {
        Channel channel = rabbitTemplate.getConnectionFactory().createConnection().createChannel(false);
        try {
            channel.queueDeclare(clientId, true, false, true, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        channel.queueBind(clientId, Const.RABBIT_EVENT_EXCHANGE, routingKey);

        SimpleConsumer consumer = new SimpleConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    handler.handle(new String(body));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    try {
                        channel.basicNack(envelope.getDeliveryTag(), false, false);
                    } catch (IOException ex) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        consumer.consumerTag  = channel.basicConsume(clientId, false, consumer);
        consumers.add(consumer);
    }

    @Override
    public void stopConsumers(String clientId) {
        for (SimpleConsumer consumer : consumers) {
            synchronized (consumer) {
                try {
                    consumer.handleCancel(consumer.consumerTag);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        consumers.clear();
    }

    static class SimpleConsumer extends DefaultConsumer {

        private String consumerTag;

        public SimpleConsumer(Channel channel) {
            super(channel);
        }
    }
}

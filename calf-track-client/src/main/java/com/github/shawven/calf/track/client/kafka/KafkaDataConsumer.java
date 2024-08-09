package com.github.shawven.calf.track.client.kafka;

import com.github.shawven.calf.track.client.DataConsumer;
import com.github.shawven.calf.track.client.DataSubscribeHandler;
import com.github.shawven.calf.track.common.Const;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xw
 * @date 2023/1/5
 */
public class KafkaDataConsumer implements DataConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaDataConsumer.class);

    KafkaListenerEndpointRegistry registry;

    ConcurrentKafkaListenerContainerFactory<?, ?> factory;

    AtomicInteger counter = new AtomicInteger(0);

    int concurrency = 16;

    public KafkaDataConsumer(KafkaListenerEndpointRegistry registry,
                             ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
        this.registry = registry;
        this.factory = factory;
    }

    @Override
    public String queueType() {
        return Const.QUEUE_TYPE_KAFKA;
    }

    @Override
    public void startConsumers(String clientId, Map<String, DataSubscribeHandler> handlerMap) {
        for (Map.Entry<String, DataSubscribeHandler> entry : handlerMap.entrySet()) {
            DataSubscribeHandler handler = entry.getValue();
            String topic = Const.kafkaTopicName(handler.namespace(), handler.dataSource(), handler.database());
            registerConsumer(clientId, topic, entry.getValue());
        }
    }

    private void registerConsumer(String clientId, String topic, DataSubscribeHandler handler){
        MethodKafkaListenerEndpoint<?, ?> endpoint = new MethodKafkaListenerEndpoint<>();

        Consumer lambada = new Consumer() {
            @Override
            public void invoke(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    handler.handle(record.value());
                }

                ack.acknowledge();
            }
        };

        Optional<Method> optional = Arrays.stream(lambada.getClass().getMethods())
                .filter(method -> "invoke".equals(method.getName()))
                .findFirst();
        if (optional.isPresent()) {
            endpoint.setMethod(optional.get());
        } else {
            throw new IllegalStateException();
        }

        endpoint.setBean(lambada);
        endpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        endpoint.setId(clientId + "#" + counter.incrementAndGet());
        endpoint.setGroupId(clientId);
        endpoint.setTopics(topic);
        endpoint.setConcurrency(concurrency);
        endpoint.setBatchListener(true);

        registry.registerListenerContainer(endpoint, factory, true);
    }


    @Override
    public void stopConsumers(String clientId) {
        registry.stop();
    }

    public interface Consumer {
        void invoke(List<ConsumerRecord<String, String>> records, Acknowledgment ack);
    }
}

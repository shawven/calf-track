package com.github.shawven.calf.track.client.kafka;

import com.github.shawven.calf.track.client.DataConsumer;
import com.github.shawven.calf.track.client.DataSubscribeHandler;
import com.github.shawven.calf.track.common.Const;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.*;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    public void startConsumers(String clientId, List<DataSubscribeHandler> handlers) {
        for (DataSubscribeHandler handler : handlers) {
            registerConsumer(clientId, handler);
        }
    }

    private void registerConsumer(String clientId, DataSubscribeHandler handler){
        MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();

        String topic = Const.partialToDb(handler.namespace(), handler.dataSource(), handler.database());
        String matchKey = Const.uniqueKey(handler.namespace(), handler.dataSource(), handler.database(), handler.table());
        endpoint.setTopics(topic);

        Optional<Method> optional = Arrays.stream(InnerListener.class.getMethods())
                .filter(method -> "onMessage".equals(method.getName()))
                .findFirst();
        if (optional.isPresent()) {
            endpoint.setMethod(optional.get());
        } else {
            throw new IllegalStateException();
        }

        endpoint.setBean(new InnerListener(handler));
        endpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        endpoint.setId(clientId + "@" + counter.incrementAndGet());
        endpoint.setGroupId(clientId);
        endpoint.setConcurrency(concurrency);
        endpoint.setBatchListener(true);
        endpoint.setRecordFilterStrategy(new RecordFilterStrategy<String, String>() {
            @Override
            public boolean filter(ConsumerRecord<String, String> record) {
                return matchKey.equals(record.key()) || matchKey.matches(record.key());
            }

            @Override
            public List<ConsumerRecord<String, String>> filterBatch(List<ConsumerRecord<String, String>> records) {
                return records.stream().filter(this::filter).collect(Collectors.toList());
            }
        });

        registry.registerListenerContainer(endpoint, factory, true);
    }


    @Override
    public void stopConsumers(String clientId) {
        registry.stop();
    }

    static class InnerListener implements Listener {

        DataSubscribeHandler handler;

        public InnerListener(DataSubscribeHandler handler) {
            this.handler = handler;
        }

        @Override
        public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
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

    public interface Listener {
        void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack);
    }
}

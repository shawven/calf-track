package com.github.shawven.calf.track.server.publisher.kafka;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.event.DataSourceWatchedEvent;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author xw
 * @date 2024/8/8
 */
public class KafkaService {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    AdminClient adminClient;

    Set<String> topics = new HashSet<>();

    public KafkaService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }



    public void getConsumerGroup() {

    }

    @EventListener(DataSourceWatchedEvent.class)
    public void onDataSourceStartedEvent(DataSourceWatchedEvent event) {
        DataSourceCfg cfg = event.getDataSourceCfg();
        String database = event.getDatabase();

        // 数据库级别的topic namespace_datasource_database
        String topic = Const.partialToDb(cfg.getNamespace(), cfg.getName(), database);

        // 创建kafka队列
        createTopic(topic, 16, 1);
    }

    public void createTopic(String topic, int partitions, int replication) {
        if (!topics.contains(topic)) {
            NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
            try {
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                topics.add(topic);
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) { // Possible race with another app instance
                    logger.debug("Failed to create topics", e.getCause());
                    return;
                }

                logger.error(e.getMessage(), e);
            }
        }
    }
}

package com.github.shawven.calf.track.server.publisher.kafka;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author xw
 * @date 2023/1/5
 */
public class KafkaDataPublisher implements DataPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDataPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaDataPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(BaseRows data) {
        String msg = JSON.toJSONString(data);
        try {
            String topic = Const.partialToDb(data.getNamespace(), data.getDsName(), data.getDatabase());
            kafkaTemplate.send(topic, data.key(), msg);
//            logger.info("推送信息 {}", msg);
        } catch (Exception e) {
            logger.error("推送信息  " + msg + " 失败", e);
        }
    }



    @Override
    public void destroy() {
        kafkaTemplate.destroy();
    }

}

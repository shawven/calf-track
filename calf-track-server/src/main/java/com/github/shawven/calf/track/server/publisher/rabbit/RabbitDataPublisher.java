package com.github.shawven.calf.track.server.publisher.rabbit;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * @author xw
 * @date 2023/1/5
 */
public class RabbitDataPublisher implements DataPublisher {

    private static final Logger logger = LoggerFactory.getLogger(RabbitDataPublisher.class);

    private final RabbitTemplate rabbitTemplate;

    public RabbitDataPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public void publish(BaseRows data) {
        String msg = JSON.toJSONString(data);
        try {
            String routingKey = Const.partialToDb(data.getNamespace(), data.getDsName(), data.getDatabase());
            rabbitTemplate.convertAndSend(Const.RABBIT_EVENT_EXCHANGE, routingKey, msg);
//            logger.info("推送信息 {}", msg);
        } catch (Exception e) {
            logger.error("推送信息  " + msg + " 失败", e);
        }
    }


    @Override
    public void destroy() {
        rabbitTemplate.destroy();
    }

}

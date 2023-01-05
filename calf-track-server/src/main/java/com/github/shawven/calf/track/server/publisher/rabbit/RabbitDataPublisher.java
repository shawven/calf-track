package com.github.shawven.calf.track.server.publisher.rabbit;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * @auther: chenjh
 * @time: 2018/11/19 14:57
 * @description
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
            String routingKey = Const.withEventQueue(data.key());

            sendData(routingKey, msg);
            logger.info("推送信息 {}", msg);
        } catch (Exception e) {
            logger.error("推送信息  " + msg + " 失败", e);
        }
    }

    private void sendData(String routingKey, String msg) {
        rabbitTemplate.convertAndSend(Const.RABBIT_EVENT_EXCHANGE, routingKey, msg);
    }


    @Override
    public void destroy() {
        rabbitTemplate.destroy();
    }

}

package com.github.shawven.calf.track.examples;

import com.github.shawven.calf.track.client.annotation.DataSubscriber;
import com.github.shawven.calf.track.common.EventAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author xw
 * @date 2023/1/5
 */
@Service
public class ExampleHandlers {

    private static final Logger logger = LoggerFactory.getLogger(ExampleHandlers.class);

    @DataSubscriber(
            dataSource = "mongo_dev",
            database = "test",
            table = "t_user",
            actions = {EventAction.INSERT, EventAction.UPDATE, EventAction.DELETE})
    public void handle1(String data) {
        logger.info("handle2 接收 rabbit 信息:" + data);
    }

    /**
     * 指定表
     *
     * @param data
     */
    @DataSubscriber(
            dataSource = "mongo_local",
            database = "test",
            table = "t_user_0",
            actions = {EventAction.INSERT, EventAction.UPDATE, EventAction.DELETE})
    public void handle2(String data) {
        logger.info("handle2 接收 kafka 信息:" + data);
    }

    /**
     * 正则匹配
     *
     * @param data
     */
    @DataSubscriber(
            dataSource = "mongo_local",
            database = "test",
            table = "t_user_\\d",
            actions = {EventAction.INSERT, EventAction.UPDATE, EventAction.DELETE})
    public void handle3(String data) {
        logger.info("handle3 接收 kafka 信息:" + data);
    }
}

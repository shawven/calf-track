package com.github.shawven.calf.track.server.autoconfig;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
@ConditionalOnProperty("spring.kafka.bootstrap-servers")
public class KafkaProperties {

    private String servers = "192.168.1.81:2181";

    private int connectionTimeout = 30000;

    private int sessionTimeout = 30000;
    /**
     * kafka默认分区数
     */
    private int partitions = 1;
    /**
     * kafka默认副本数
     */
    private int replications = 1;

}

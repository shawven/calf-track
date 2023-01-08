package com.github.shawven.calf.track.register.zookeeper;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
@ConfigurationProperties("track.zookeeper")
public class ZkProperties {

    private String url;

    private String root;
}

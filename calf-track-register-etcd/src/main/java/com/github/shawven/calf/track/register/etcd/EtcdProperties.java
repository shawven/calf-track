package com.github.shawven.calf.track.register.etcd;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
@ConfigurationProperties("track.etcd")
public class EtcdProperties {

    private String url;

    private String authority;

    private String username;

    private String password;
}

package com.github.shawven.calf.track.register.autoconfig;

import com.github.shawven.calf.track.register.PathKey;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author xw
 * @date 2023/1/5
 */
@Configuration(proxyBeanMethods = false)
class TrackRegisterAutoConfiguration implements InitializingBean {

    @Value("${calf-track.root}") String root;

    @Override
    public void afterPropertiesSet() throws Exception {
        PathKey.setRoot(root);
    }
}

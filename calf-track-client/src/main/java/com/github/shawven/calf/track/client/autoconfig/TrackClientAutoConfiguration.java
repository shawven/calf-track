package com.github.shawven.calf.track.client.autoconfig;

import com.github.shawven.calf.track.client.DataSubscribeRegistry;
import com.github.shawven.calf.track.client.annotation.DataListenerAnnotationBeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author xw
 * @date 2023/1/5
 */
@Configuration(proxyBeanMethods = false)
class TrackClientAutoConfiguration {

    @Bean
    public DataListenerAnnotationBeanPostProcessor dataListenerAnnotationBeanPostProcessor(
            DataSubscribeRegistry dataSubscribeRegistry) {
        return new DataListenerAnnotationBeanPostProcessor(dataSubscribeRegistry);
    }
}

package com.github.shawven.calf.track.register.zookeeper;

import com.github.shawven.calf.track.register.ElectionFactory;
import com.github.shawven.calf.track.register.Repository;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(CuratorFramework.class)
@EnableConfigurationProperties(ZkProperties.class)
class ZkConfiguration {

    @Bean
    public CuratorFramework curatorFramework(ZkProperties zkProperties) {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkProperties.getUrl())
                .retryPolicy(new RetryNTimes(3, 500))
                .build();
        client.start();
        return client;
    }

    @Bean
    @ConditionalOnMissingBean(ElectionFactory.class)
    public ElectionFactory electionFactory(CuratorFramework client) {
        return new ZkElectionFactory(client);
    }



    @Bean
    public Repository etcdDataSource(CuratorFramework client) {
        return new ZkRepository(client);
    }
}

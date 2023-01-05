package com.github.shawven.calf.track.register.etcd;

import com.github.shawven.calf.track.register.ElectionFactory;
import com.github.shawven.calf.track.register.Repository;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.Util;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(Client.class)
@EnableConfigurationProperties(EtcdProperties.class)
class TrackRegisterEtcdConfiguration {

    @Bean
    public Client etcdClient(EtcdProperties etcdProperties) {
        return Client
                .builder()
                .endpoints(Util.toURIs(Arrays.asList(etcdProperties.getUrl().split(","))))
                //.authority(authority)
                //.user(ByteSequence.from(username, StandardCharsets.UTF_8))
                //.password(ByteSequence.from(password, StandardCharsets.UTF_8))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(ElectionFactory.class)
    public ElectionFactory electionFactory(Client client) {
        return new EtcdElectionFactory(client);
    }



    @Bean
    public Repository etcdDataSource(Client client) {
        return new EtcdRepository(client);
    }
}

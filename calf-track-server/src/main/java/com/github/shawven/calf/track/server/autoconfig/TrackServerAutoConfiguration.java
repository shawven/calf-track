package com.github.shawven.calf.track.server.autoconfig;

import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.TrackServer;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.Repository;
import com.github.shawven.calf.track.server.TrackServerRunner;
import com.github.shawven.calf.track.server.ops.ClientOpsImpl;
import com.github.shawven.calf.track.server.ops.DataSourceCfgOpsImpl;
import com.github.shawven.calf.track.server.ops.StatusOpsImpl;
import com.github.shawven.calf.track.server.publisher.DataPublisherManager;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter({TrackDataPublisherAutoConfiguration.class})
class TrackServerAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackages = "com.github.shawven.calf.track.server.web")
    static class webSupportConfiguration { }

    @Bean
    @ConditionalOnMissingBean(ClientOps.class)
    public ClientOps clientDAOImpl(Repository repository, DataSourceCfgOps dataSourceCfgOps) {
        return new ClientOpsImpl(repository, dataSourceCfgOps);
    }

    @Bean
    @ConditionalOnMissingBean(StatusOps.class)
    public StatusOps statusDAOImpl(Repository repository, DataSourceCfgOps dataSourceCfgOps) {
        return new StatusOpsImpl(repository, dataSourceCfgOps);
    }

    @Bean
    @ConditionalOnMissingBean(DataSourceCfgOps.class)
    public DataSourceCfgOps dataSourceCfgDAOImpl(Repository repository) {
        return new DataSourceCfgOpsImpl(repository);
    }

    @Bean
    @Primary
    public DataPublisherManager dataPublisherManager(Map<String, DataPublisher> dataPublisherMap){
        return new DataPublisherManager(dataPublisherMap);
    }

    @Bean
    public TrackServerRunner serverRunner(List<TrackServer> serverList) {
        return new TrackServerRunner(serverList);
    }
}

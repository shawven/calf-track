package com.github.shawven.calf.track.server.autoconfig;

import com.github.shawven.calf.track.datasource.api.TrackServer;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.Repository;
import com.github.shawven.calf.track.server.TrackServerRunner;
import com.github.shawven.calf.track.server.ops.ClientOpsImpl;
import com.github.shawven.calf.track.server.ops.DataSourceCfgOpsImpl;
import com.github.shawven.calf.track.server.ops.StatusOpsImpl;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Objects;

@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter({TrackDataPublisherAutoConfiguration.class})
class TrackServerAutoConfiguration implements InitializingBean {

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackages = "com.github.shawven.calf.track.server.web")
    static class webSupportConfiguration { }

    @Value("${track.root:}") String root;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!Objects.equals(root, "")) {
            PathKey.init(root);
        }
    }

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
    public TrackServerRunner serverRunner(List<TrackServer> serverList) {
        return new TrackServerRunner(serverList);
    }
}

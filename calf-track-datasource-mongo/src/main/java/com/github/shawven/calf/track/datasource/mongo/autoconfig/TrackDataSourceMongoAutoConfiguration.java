package com.github.shawven.calf.track.datasource.mongo.autoconfig;

import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.TrackServer;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.datasource.mongo.MongoTrackServerImpl;
import com.github.shawven.calf.track.datasource.mongo.OpLogClientFactory;
import com.github.shawven.calf.track.register.ElectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author xw
 * @date 2023-01-05
 */
@Configuration(proxyBeanMethods = false)
class TrackDataSourceMongoAutoConfiguration {

    @Bean
    public OpLogClientFactory opLogClientFactory(StatusOps statusOps,
                                                 DataSourceCfgOps dataSourceCfgOps) {
        return new OpLogClientFactory(statusOps, dataSourceCfgOps);
    }

    @Bean
    public TrackServer mongoReplicationServerImpl(OpLogClientFactory opLogClientFactory,
                                                  ElectionFactory electionFactory,
                                                  ClientOps clientOps,
                                                  StatusOps statusOps,
                                                  DataSourceCfgOps dataSourceCfgOps,
                                                  DataPublisher dataPublisher) {
        return new MongoTrackServerImpl(opLogClientFactory, electionFactory, dataSourceCfgOps,
                clientOps, statusOps, dataPublisher);
    }

}

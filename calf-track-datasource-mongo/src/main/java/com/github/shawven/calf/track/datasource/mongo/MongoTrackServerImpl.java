package com.github.shawven.calf.track.datasource.mongo;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.AbstractTrackServer;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.NetUtils;
import com.github.shawven.calf.track.register.ElectionFactory;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.InstanceStatus;
import com.github.shawven.calf.track.register.election.Election;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author xw
 * @date 2023-01-05
 */
public class MongoTrackServerImpl extends AbstractTrackServer {

    public static final String TYPE = "MongoDB";

    private final OpLogClientFactory opLogClientFactory;

    private final DataSourceCfgOps dataSourceCfgOps;

    private final DataPublisher dataPublisherManager;

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final ElectionFactory electionFactory;

    private final Map<String, Election> electionMap = new ConcurrentHashMap<>();

    public MongoTrackServerImpl(OpLogClientFactory opLogClientFactory, ElectionFactory electionFactory,
                                DataSourceCfgOps dataSourceCfgOps, ClientOps clientOps, StatusOps statusOps,
                                DataPublisher dataPublisherManager) {
        super(dataSourceCfgOps, clientOps, statusOps);
        this.opLogClientFactory = opLogClientFactory;
        this.electionFactory = electionFactory;
        this.dataSourceCfgOps = dataSourceCfgOps;
        this.dataPublisherManager = dataPublisherManager;
    }

    @Override
    public String dataSourceType() {
        return TYPE;
    }

    @Override
    public void doStart(DataSourceCfg dataSourceCfg) {
        executorService.submit(() -> {
            String path = PathKey.concat(Const.LEADER_PATH);
            String namespace = dataSourceCfg.getNamespace();
            String uniqueId = NetUtils.getLocalAddress().getHostAddress();

            OplogElectionListener listener = new OplogElectionListener(dataSourceCfg,
                    opLogClientFactory, statusOps, clientOps, dataSourceCfgOps, dataPublisherManager);

            Election election = electionFactory.getElection(path, namespace, uniqueId, 20L, listener);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("shutdownHook trigger close");
                election.close();
            }));

            electionMap.put(namespace, election);
            election.start();
        });
    }

    @Override
    public void stop(String namespace) {
        Election election = electionMap.get(namespace);
        if(election != null) {
            election.close();
        }
    }

    @Override
    protected void updateInstanceStatus() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            InstanceStatus instanceStatus = new InstanceStatus();
            String localIp = TYPE + ":" + NetUtils.getLocalAddress().getHostAddress();
            instanceStatus.setIp(localIp);
            instanceStatus.setTotalEventCount(opLogClientFactory.getEventCount());
            instanceStatus.setLatelyEventCount(opLogClientFactory.eventCountSinceLastTime());
            instanceStatus.setTotalPublishCount(dataPublisherManager.getPublishCount());
            instanceStatus.setLatelyPublishCount(dataPublisherManager.publishCountSinceLastTime());
            instanceStatus.setUpdateTime(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

            statusOps.updateInstanceStatus(localIp, instanceStatus);
            logger.info("updateInstanceStatus: [{}]", instanceStatus);
        }, 10, 20, TimeUnit.SECONDS);
    }
}

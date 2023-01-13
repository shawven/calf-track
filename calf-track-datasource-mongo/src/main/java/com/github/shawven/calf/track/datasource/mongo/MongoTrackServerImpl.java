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
import com.github.shawven.calf.track.register.domain.ServerStatus;
import com.github.shawven.calf.track.register.election.Election;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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

    private final Map<String, Long> activeDsNames = new ConcurrentHashMap<>();

    private final AtomicLong activeDsNumber = new AtomicLong();

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
    public void doStart(DataSourceCfg dsCfg) {
        executorService.submit(() -> {
            String path = PathKey.concat(Const.LEADER);
            String namespace = dsCfg.getNamespace();
            String processName = dsCfg.getName() + ":" + ManagementFactory.getRuntimeMXBean().getName();

            OplogElectionListener listener = new OplogElectionListener(dsCfg,
                    opLogClientFactory, statusOps, clientOps, dataSourceCfgOps, dataPublisherManager);

            Election election = electionFactory.getElection(path, namespace, processName, 20L, listener);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("shutdownHook trigger close");
                election.close();
            }, "TrackServer"));

            electionMap.put(namespace + "-" + dsCfg.getName(), election);

            election.start();

            activeDsNames.put(namespace + "#" + dsCfg.getName(), activeDsNumber.incrementAndGet());
        });
    }

    @Override
    public void doStop(String namespace, String name) {
        activeDsNames.remove(namespace + "#" + name);
        Election election = electionMap.get(namespace + "-" + name);

        if (election != null) {
            election.close();
        }
    }

    @Override
    protected void updateServerStatus() {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            ServerStatus serverStatus = new ServerStatus();
            serverStatus.setMachine(ManagementFactory.getRuntimeMXBean().getName());
            serverStatus.setIp(NetUtils.getLocalAddress().getHostAddress());
            serverStatus.setActiveDsNames(getSortedActiveDsNames());
            serverStatus.setTotalEventCount(opLogClientFactory.getEventCount());
            serverStatus.setLatelyEventCount(opLogClientFactory.eventCountSinceLastTime());
            serverStatus.setTotalPublishCount(dataPublisherManager.getPublishCount());
            serverStatus.setLatelyPublishCount(dataPublisherManager.publishCountSinceLastTime());
            serverStatus.setUpdateTime(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));

            String key = serverStatus.getMachine() + "-" + TYPE;
            statusOps.updateServerStatus(key, serverStatus);

            logger.debug("updateInstanceStatus: [{}]", serverStatus);
        }, 0, 5, TimeUnit.SECONDS);
    }

    private List<String> getSortedActiveDsNames() {
        TreeMap<Long, String> treeMap = new TreeMap<>();
        activeDsNames.forEach((key, val) -> {
            treeMap.put(val, key.split("#")[1]);
        });
        return new ArrayList<>(treeMap.values());
    }
}

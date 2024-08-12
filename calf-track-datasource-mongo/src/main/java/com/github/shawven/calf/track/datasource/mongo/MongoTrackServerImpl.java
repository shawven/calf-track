package com.github.shawven.calf.track.datasource.mongo;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
import com.github.shawven.calf.track.datasource.api.AbstractTrackServer;
import com.github.shawven.calf.track.datasource.api.BaseWorkSpace;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.NetUtils;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import com.github.shawven.calf.track.register.ElectionFactory;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.ServerStatus;
import com.github.shawven.calf.track.register.election.Election;
import com.github.shawven.calf.track.register.election.ElectionListener;
import io.reactivex.rxjava3.disposables.Disposable;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xw
 * @date 2023-01-05
 */
public class MongoTrackServerImpl extends AbstractTrackServer implements ElectionListener{

    public static final String TYPE = "MongoDB";

    private final OpLogClientFactory opLogClientFactory;

    private final DataSourceCfgOps dataSourceCfgOps;

    private final DataPublisher dataPublisher;

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final ElectionFactory electionFactory;

    private final Map<String, Election> electionMap = new ConcurrentHashMap<>();

    private final Map<String, Long> activeDsNames = new ConcurrentHashMap<>();

    private final AtomicLong activeDsNumber = new AtomicLong();

    public MongoTrackServerImpl(OpLogClientFactory opLogClientFactory, ElectionFactory electionFactory,
                                DataSourceCfgOps dataSourceCfgOps, ClientOps clientOps, StatusOps statusOps,
                                DataPublisher dataPublisher) {
        super(dataSourceCfgOps, clientOps, statusOps);
        this.opLogClientFactory = opLogClientFactory;
        this.electionFactory = electionFactory;
        this.dataSourceCfgOps = dataSourceCfgOps;
        this.dataPublisher = dataPublisher;
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

            ElectionListener listener = new OplogWorker(applicationContext, dsCfg);


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
            serverStatus.setTotalPublishCount(dataPublisher.getPublishCount());
            serverStatus.setLatelyPublishCount(dataPublisher.publishCountSinceLastTime());
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

    private class OplogWorker extends BaseWorkSpace {

        private final Logger logger = LoggerFactory.getLogger(getClass());

        private OplogClient oplogClient;

        private Disposable disposable;

        public OplogWorker(ApplicationContext applicationContext,
                           DataSourceCfg dataSourceCfg) {
            super(applicationContext, dataSourceCfg);
        }

        @Override
        public void isLeader() {
            oplogClient = opLogClientFactory.initClient(dataSourceCfg);
            String namespace = dataSourceCfg.getNamespace();
            String dsName = dataSourceCfg.getName();
            String destQueue = dataSourceCfg.getDestQueue();

            // 更新关注的事件
            updateWatchedEvents(clientOps.listConsumerClientsByNamespaceAndName(namespace, dsName));

            // 监听Client列表变化，更新关注的事件
            clientOps.watcherClientInfo(dataSourceCfg, this::updateWatchedEvents);

            OpLogEventFormatterFactory formatterFactory = new OpLogEventFormatterFactory(namespace, dsName, destQueue);

            // 启动连接
            try {
                disposable = oplogClient.getOplog().subscribe(document -> {
                    opLogClientFactory.incEventCount();

                    // 获取数据格式器
                    String eventType = document.getString(OpLogClientFactory.EVENTTYPE_KEY);
                    OpLogEventFormatter formatter = formatterFactory.getFormatter(eventType);

                    // 处理
                    handle(document, formatter);

                    // 更新状态
                    updateOpLogStatus(document);
                });
                dataSourceCfg.setActive(true);
                dataSourceCfg.setMachine(ManagementFactory.getRuntimeMXBean().getName());
                dataSourceCfg.setVersion(dataSourceCfg.getVersion() + 1);
                dataSourceCfgOps.update(dataSourceCfg);
            } catch (Exception e) {
                logger.error("[" + dataSourceCfg.getNamespace() + "] 处理事件异常", e);
            }

        }

        @Override
        public void notLeader() {
            if (disposable != null) {
                disposable.dispose();
            }

            dataSourceCfg.setActive(false);
            dataSourceCfg.setMachine("");
            dataSourceCfgOps.update(dataSourceCfg);
            opLogClientFactory.closeClient(oplogClient, dataSourceCfg);
        }

        /**
         * 处理event
         *
         * @param event
         * @param formatter
         */
        private void handle(Document event, OpLogEventFormatter formatter) {
            if (!filterDocument(event)) {
                return;
            }

            BaseRows formatData = formatter.format(event);
            if (formatData == null) {
                logger.debug("uninterested:{}", event);
                return;
            }

            try {
                MongoTrackServerImpl.this.dataPublisher.publish(formatData);
            } catch (Exception e) {
                logger.error("dataPublisherManager.publish error: " + e.getMessage(), e);
            }
        }

        /**
         * 更新日志位置
         *
         * @param document
         */
        protected void updateOpLogStatus(Document document) {
            BsonTimestamp ts = (BsonTimestamp) document.get(OpLogClientFactory.TIMESTAMP_KEY);
            statusOps.updateDataSourceStatus(String.valueOf(ts.getTime()), ts.getInc(), dataSourceCfg);
        }

        protected boolean filterDocument(Document event) {
            EventAction eventAction = parseEventAction(event.getString(OpLogClientFactory.EVENTTYPE_KEY));
            if (eventAction == null) {
                return false;
            }
            String dataBase = DocumentUtils.getDataBase(event);
            String table = DocumentUtils.getTable(event);
            return filterEvent(eventAction, dataBase, table);
        }

        private  EventAction parseEventAction(String eventType) {
            EventAction databaseEvent = null;
            switch (eventType) {
                case "i":
                    databaseEvent = EventAction.INSERT;
                    break;
                case "u":
                    databaseEvent = EventAction.UPDATE;
                    break;
                case "d":
                    databaseEvent = EventAction.DELETE;
                    break;
                default:
            }
            return databaseEvent;
        }
    }
}

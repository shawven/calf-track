package com.github.shawven.calf.track.datasource.mongo;

import com.github.shawven.calf.track.common.EventAction;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.datasource.api.BaseWorkSpace;
import io.reactivex.rxjava3.disposables.Disposable;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.management.ManagementFactory;


/**
 * @author xw
 * @date 2023-01-05
 */
public class OplogWorker extends BaseWorkSpace {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final OpLogClientFactory opLogClientFactory;

    private final StatusOps statusOps;

    private final ClientOps clientOps;

    private final DataSourceCfgOps dataSourceCfgOps;

    private final DataPublisher dataPublisher;

    private OplogClient oplogClient;

    private Disposable disposable;

    public OplogWorker(ApplicationContext applicationContext,
                       DataSourceCfg dataSourceCfg,
                       OpLogClientFactory opLogClientFactory,
                       StatusOps statusOps, ClientOps clientOps,
                       DataSourceCfgOps dataSourceCfgOps,
                       DataPublisher dataPublisher) {
        super(applicationContext, dataSourceCfg);
        this.opLogClientFactory = opLogClientFactory;
        this.statusOps = statusOps;
        this.clientOps = clientOps;
        this.dataSourceCfgOps = dataSourceCfgOps;
        this.dataPublisher = dataPublisher;
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
            dataPublisher.publish(formatData);
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

    private static EventAction parseEventAction(String eventType) {
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

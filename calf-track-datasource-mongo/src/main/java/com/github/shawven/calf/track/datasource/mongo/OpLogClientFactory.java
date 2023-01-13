package com.github.shawven.calf.track.datasource.mongo;


import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.DataSourceStatus;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xw
 * @date 2023-01-05
 */
@Component
public class OpLogClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(OpLogClientFactory.class);

    private final StatusOps statusOps;

    private final DataSourceCfgOps dataSourceCfgOps;

    private final AtomicLong eventCount = new AtomicLong(0);

    private long lastEventCount = 0;

    /**
     * 事件类型的key，值可为i(插入)、u(更新)、d(删除)
     */
    public static final String EVENTTYPE_KEY = "op";
    /**
     * 数据库、集合名称
     */
    public static final String DATABASE_KEY = "ns";
    /**
     * 消费时间戳
     */
    public static final String TIMESTAMP_KEY = "ts";
    /**
     * 内容
     */
    public static final String CONTEXT_KEY = "o";
    /**
     * 更新事件更新的条件
     */
    public static final String UPDATE_WHERE_KEY = "o2";
    /**
     * 更新事件，更新的内容
     */
    public static final String UPDATE_CONTEXT_KEY = "$set";

    public OpLogClientFactory(StatusOps statusOps,
                              DataSourceCfgOps dataSourceCfgOps) {
        this.statusOps = statusOps;
        this.dataSourceCfgOps = dataSourceCfgOps;
    }

    public OplogClient initClient(DataSourceCfg dataSourceCfg) {
        MongoClient mongoClient = MongoClients.create(dataSourceCfg.getDataSourceUrl());

        OplogClient client = new OplogClient(mongoClient);

        // 配置当前位置
        configOpLogStatus(client, dataSourceCfg);

        return client;
    }

    /**
     * 配置当前位置
     *
     * @param oplogClient
     * @param dataSourceCfg
     */
    private void configOpLogStatus(OplogClient oplogClient, DataSourceCfg dataSourceCfg) {
        DataSourceStatus dataSourceStatus = statusOps.getDataSourceStatus(dataSourceCfg);
        if (dataSourceStatus != null) {
            int seconds = Integer.parseInt(String.valueOf(dataSourceStatus.getFilename()));
            int inc = Integer.parseInt((String.valueOf(dataSourceStatus.getPosition())));
            oplogClient.setTs(new BsonTimestamp(seconds, inc));
        }
    }

    public void closeClient(OplogClient oplogClient, DataSourceCfg namespace) {
        try {
            // remove active namespace
            oplogClient.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return;
        }
        DataSourceCfg dataSourceCfg = dataSourceCfgOps.get(namespace.getNamespace(), namespace.getName());
        try {
            while (dataSourceCfg.isActive()) {
                TimeUnit.SECONDS.sleep(1);
                dataSourceCfg = dataSourceCfgOps.get(namespace.getNamespace(), namespace.getName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage(), e);
        }
    }


    public void incEventCount() {
        eventCount.incrementAndGet();
    }

    public long getEventCount() {
        return eventCount.get();
    }

    public long eventCountSinceLastTime() {
        long total = eventCount.get();
        long res = total - lastEventCount;

        lastEventCount = total;

        return res;

    }
}

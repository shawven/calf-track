package com.github.shawven.calf.track.datasource.api.domain;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class BaseRows implements Serializable {

    private static final long serialVersionUID = -188511150756468947L;

    /**
     * 确保传输的数据唯一
     */
    private String uuid = UUID.randomUUID().toString();

    /**
     * 命名空间
     */
    private String namespace;

    /**
     * 数据源的名称
     */
    private String dsName;

    /**
     * 目标队列
     */
    private String destQueue;

    /**
     * 事件动作
     */
    private EventAction eventAction;

    /**
     * 数据库
     */
    private String database;

    /**
     * 表名
     */
    private String table;


    private long timestamp = System.currentTimeMillis();


    public BaseRows(String namespace, String dsName, String destQueue, EventAction eventAction, String database, String table) {
        this.namespace = namespace;
        this.dsName = dsName;
        this.destQueue = destQueue;
        this.eventAction = eventAction;
        this.database = database;
        this.table = table;
    }

    public String key() {
        return Const.uniqueKey(namespace, dsName, database, table);
    }
}

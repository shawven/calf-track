package com.github.shawven.calf.track.datasource.api.domain;


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
     * 目标队列
     */
    private String destQueue;

    /**
     * 时间动作
     */
    private EventAction eventAction;

    private String database;

    private String table;

    private String dsName;

    private Long timestamp = System.currentTimeMillis();

    public BaseRows() {
    }

    public BaseRows(String dsName, EventAction eventAction, String database, String table) {
        this.dsName = dsName;
        this.eventAction = eventAction;
        this.database = database;
        this.table = table;
    }

    public String key() {
        return dsName + "_" + database + "_" + table;
    }
}

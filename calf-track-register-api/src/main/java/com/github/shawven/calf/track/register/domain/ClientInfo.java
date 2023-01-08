package com.github.shawven.calf.track.register.domain;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class ClientInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 名称
     */
    private String name;

    /**
     * 队列实现方式 默认为redis
     */
    private String queueType;

    /**
     * 关注的数据库的标识
     */
    private String dsName;

    /**
     * 关注的数据库名
     */
    private String databaseName;

    /**
     * 关注的表名
     */
    private String tableName;

    /**
     * 关注的表的事件
     */
    private List<EventAction> eventActions;

    /**
     * 拼接key，避免频繁拼接
     */
    private String key;


    public ClientInfo(String name, String queueType,
                      String dsName, String databaseName,
                      String tableName, List<EventAction> eventActions) {
        this.name = name;
        this.queueType = queueType;
        this.dsName = dsName == null ? Const.NAMESPACE : dsName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.eventActions = eventActions;
        this.key = this.dsName + "_" + databaseName + "_" + tableName;
    }
}

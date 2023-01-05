package com.github.shawven.calf.track.register.domain;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
import lombok.Data;

import java.io.Serializable;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class ClientInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 客户端编号
     */
    private String clientId;

    /**
     * 队列实现方式 默认为redis
     */
    private String queueType;

    /**
     * 关注的数据库的标识
     */
    private String namespace = Const.DEFAULT_NAMESPACE;

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
    private EventAction eventAction;

    /**
     * 拼接key，避免频繁拼接
     */
    private String key;

    public ClientInfo() {}

    public ClientInfo(String clientId, String queueType,
                      String namespace, String databaseName,
                      String tableName, EventAction eventAction) {
        this.clientId = clientId;
        this.queueType = queueType;
        this.namespace = namespace == null ? Const.DEFAULT_NAMESPACE : namespace;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.eventAction = eventAction;
        this.key = this.namespace + "_" + databaseName + "_" + tableName;
    }
}

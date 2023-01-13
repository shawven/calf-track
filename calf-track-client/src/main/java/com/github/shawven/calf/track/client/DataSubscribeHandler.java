package com.github.shawven.calf.track.client;


import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
/**
 * @author xw
 * @date 2023-01-05
 */
public interface DataSubscribeHandler {

    /**
     * 命名空间(数据隔离)
     *
     * @return
     */
    default String namespace() {
        return Const.NAMESPACE;
    }

    /**
     * 数据源
     *
     * @return
     */
    String dataSource();

    /**
     * 数据库
     *
     * @return
     */
    String database();

    /**
     * 表
     *
     * @return
     */
    String table();

    /**
     * 事件动作
     *
     * @return
     */
    EventAction[] actions();

    /**
     * 处理方法
     *
     * @param data
     */
    void handle(String data);
}

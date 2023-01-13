package com.github.shawven.calf.track.register.domain;

import lombok.Data;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class DataSourceCfg {

    /**
     * 名称
     */
    private String name;

    /**
     * 命名空间
     */
    private String namespace;

    /**
     * 数据源url
     */
    private String dataSourceUrl;

    /**
     * 订阅的数据源类型
     */
    private String dataSourceType;

    /**
     * 激活状态
     */
    private volatile boolean active;

    /**
     * 运行机器
     */
    private String machine;

    /**
     * 目标队列
     */
    private String destQueue;

    /**
     * 版本号
     */
    private Integer version = 0;
}

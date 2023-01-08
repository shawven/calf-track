package com.github.shawven.calf.track.datasource.api;


/**
 * @author xw
 * @date 2023-01-05
 */
public interface TrackServer {
    /**
     * 数据源类型
     *
     * @return
     */
    String dataSourceType();

    void start();

    void stop();
}

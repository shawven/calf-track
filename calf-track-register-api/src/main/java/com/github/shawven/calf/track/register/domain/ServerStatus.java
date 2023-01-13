package com.github.shawven.calf.track.register.domain;


import lombok.Data;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class ServerStatus {

    /**
     * 机器节点
     */
    private String machine;

    /**
     * ip
     */
    private String ip;

    /**
     * 激活的数据源
     */
    private List<String> activeDsNames;

    private long totalEventCount;

    private long latelyEventCount;

    private long totalPublishCount;

    private long latelyPublishCount;

    private String updateTime;
}

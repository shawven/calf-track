package com.github.shawven.calf.track.register.domain;


import lombok.Data;

import java.util.Set;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class InstanceStatus {

    private String ip;

    private Set<String> activeNamespaces;

    private long totalEventCount;

    private long latelyEventCount;

    private long totalPublishCount;

    private long latelyPublishCount;

    private String updateTime;
}

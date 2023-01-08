package com.github.shawven.calf.track.register.domain;


import lombok.Data;

import java.util.Set;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class DataSourceStatus {

    private String name;

    private String namespace;

    private String filename;

    private long position;

    private long timestamp;

    private String dateTime;
}

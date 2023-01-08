package com.github.shawven.calf.track.datasource.api.domain;

import lombok.Data;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class Command {

    private String namespace;

    private String name;

    private String delegatedIp;

    private Type type;

    public Command(String namespace, String name, Type type) {
        this.name = name;
        this.namespace = namespace;
        this.type = type;
    }


    public enum Type {

        /**
         * 开启数据源
         */
        START,

        /**
         * 开启数据源
         */
        STOP;
    }
}

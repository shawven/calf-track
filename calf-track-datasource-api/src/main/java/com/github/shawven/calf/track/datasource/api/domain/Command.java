package com.github.shawven.calf.track.datasource.api.domain;

/**
 * @author xw
 * @date 2023-01-05
 */
public class Command {

    private String namespace;

    private String delegatedIp;

    private Type type;

    public Command() {}

    public Command(String namespace, Type type) {
        this.namespace = namespace;
        this.type = type;
    }

    public Command(String namespace, String delegatedIp, Type type) {
        this.namespace = namespace;
        this.delegatedIp = delegatedIp;
        this.type = type;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getDelegatedIp() {
        return delegatedIp;
    }

    public void setDelegatedIp(String delegatedIp) {
        this.delegatedIp = delegatedIp;
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

package com.github.shawven.calf.track.common;

/**
 * @author xw
 * @date 2023-01-05
 */
public class Const {

    public static final String NAMESPACE = "default";

    public static final String COMMAND = "command";

    public static final String DATA_SOURCE = "data-source";

    public static final String LEADER = "leader";

    public static final String SERVER_STATUS =  "server-status";

    // 队列类型
    public static final String QUEUE_TYPE_REDIS = "redis";

    public static final String QUEUE_TYPE_RABBIT = "rabbit";

    public static final String QUEUE_TYPE_KAFKA = "kafka";


    /**
     * 数据源状态key
     */
    public static final String STATUS_KEY = "status";

    /**
     * 客户端列表key
     */
    public static final String CLIENT_SET_KEY = "clientSet";

    /**
     * 数据源前缀
     */
    public static final String PREFIX = "track";

    // rabbit
    public static final String RABBIT_EVENT_EXCHANGE = PREFIX;


    // redis
    public static final String REDIS_PREFIX = PREFIX + "::";


    /**
     * 事件数据队列前缀
     */
    private static final String EVENT_QUEUE_PREFIX = PREFIX;

    public static String withEventQueue(String suffix) {
        return EVENT_QUEUE_PREFIX + "_" + suffix;
    }

    public static String uniqueKey(String namespace, String dsName, String dbName, String tableName) {
        return namespace + "#" + dsName + "@" + dbName + "-" + tableName;
    }
}

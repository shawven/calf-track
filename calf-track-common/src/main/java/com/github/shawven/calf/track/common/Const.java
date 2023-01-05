package com.github.shawven.calf.track.common;

/**
 * @author xw
 * @date 2023-01-05
 */
public class Const {

    public static final String DEFAULT_NAMESPACE = "default";

    public static final String COMMAND = "command";

    public static final String NODE_CONFIG = "node-config";

    public static final String LEADER_PATH = "leader-selector";

    public static final String SERVICE_STATUS_PATH =  "service-status";

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
    public static final String DATA_SOURCE = "oplog";

    // rabbit
    public static final String RABBIT_EVENT_EXCHANGE = DATA_SOURCE;


    // redis
    public static final String REDIS_PREFIX = DATA_SOURCE + "::";


    /**
     * 事件数据队列前缀
     */
    private static final String EVENT_QUEUE_PREFIX = DATA_SOURCE;

    public static String withEventQueue(String suffix) {
        return EVENT_QUEUE_PREFIX + "_" + suffix;
    }
}

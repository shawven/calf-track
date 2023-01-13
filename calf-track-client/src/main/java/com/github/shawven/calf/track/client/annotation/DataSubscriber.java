package com.github.shawven.calf.track.client.annotation;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;

import java.lang.annotation.*;
/**
 * @author xw
 * @date 2023-01-05
 */
@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DataSubscriber {

    /**
     * 命名空间(数据隔离)
     *
     * @return
     */
    String namespace() default Const.NAMESPACE;

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
}

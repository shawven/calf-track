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

    String name() default Const.NAMESPACE;

    String database();

    String table();

    EventAction[] actions();
}

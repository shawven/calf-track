package com.github.shawven.calf.track.client.annotation;

import com.github.shawven.calf.track.client.DataSubscribeHandler;
import com.github.shawven.calf.track.client.DataSubscriberMethodAdapter;
import com.github.shawven.calf.track.client.DataSubscribeRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;


public class DataListenerAnnotationBeanPostProcessor implements SmartInitializingSingleton, BeanPostProcessor {

    private final DataSubscribeRegistry dataSubscribeRegistry;

    public DataListenerAnnotationBeanPostProcessor(DataSubscribeRegistry dataSubscribeRegistry) {
        this.dataSubscribeRegistry = dataSubscribeRegistry;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Assert.state(dataSubscribeRegistry != null, "dataListenerRegistry must be set");

    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * @param bean
     * @param beanName
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Map<Method, DataSubscriber> annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
                (MethodIntrospector.MetadataLookup<DataSubscriber>) method -> {
                    return AnnotatedElementUtils.getMergedAnnotation(method, DataSubscriber.class);
                });
        // 处理注解方法
        if (!annotatedMethods.isEmpty()) {
            annotatedMethods.forEach((method, ann) -> {
                DataSubscriberMethodAdapter methodAdapter = new DataSubscriberMethodAdapter(bean, method, ann);
                dataSubscribeRegistry.registerHandler(methodAdapter);
            });
        } else {
            if (DataSubscribeHandler.class.isAssignableFrom(AopUtils.getTargetClass(bean))) {
                dataSubscribeRegistry.registerHandler((DataSubscribeHandler) bean);
            }
        }

        return bean;
    }
}

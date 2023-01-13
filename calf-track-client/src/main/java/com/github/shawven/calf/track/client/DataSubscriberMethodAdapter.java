package com.github.shawven.calf.track.client;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.common.EventAction;
import com.github.shawven.calf.track.client.annotation.DataSubscriber;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * @author xw
 * @date 2023/1/3
 */
public class DataSubscriberMethodAdapter implements DataSubscribeHandler {

    private final Object bean;
    private final Method method;
    private final DataSubscriber ann;
    private final EventAction[] actions;

    public DataSubscriberMethodAdapter(Object bean, Method method, DataSubscriber ann) {
        this.bean = bean;
        this.method = method;
        this.ann = ann;
        this.actions = ann.actions();
    }

    @Override
    public String namespace() {
        return ann.namespace();
    }

    @Override
    public String dataSource() {
        return ann.dataSource();
    }

    @Override
    public String database() {
        return ann.database();
    }

    @Override
    public String table() {
        return ann.table();
    }

    @Override
    public EventAction[] actions() {
        return actions;
    }

    @Override
    public void handle(String data) {
        doInvoke(data);
    }

    /**
     * Invoke the data subscriber method with the given argument values.
     */
    @Nullable
    protected void doInvoke(Object... args) {

        ReflectionUtils.makeAccessible(this.method);
        try {
            this.method.invoke(bean, args);
        }
        catch (IllegalArgumentException ex) {
            assertTargetBean(this.method, bean, args);
            throw new IllegalStateException(getInvocationErrorMessage(bean, ex.getMessage(), args), ex);
        }
        catch (IllegalAccessException ex) {
            throw new IllegalStateException(getInvocationErrorMessage(bean, ex.getMessage(), args), ex);
        }
        catch (InvocationTargetException ex) {
            // Throw underlying exception
            Throwable targetException = ex.getTargetException();
            if (targetException instanceof RuntimeException) {
                throw (RuntimeException) targetException;
            }
            else {
                String msg = getInvocationErrorMessage(bean, "Failed to invoke data subscriber method", args);
                throw new UndeclaredThrowableException(targetException, msg);
            }
        }
    }

    private void assertTargetBean(Method method, Object targetBean, Object[] args) {
        Class<?> methodDeclaringClass = method.getDeclaringClass();
        Class<?> targetBeanClass = targetBean.getClass();
        if (!methodDeclaringClass.isAssignableFrom(targetBeanClass)) {
            String msg = "The data subscriber method class '" + methodDeclaringClass.getName() +
                    "' is not an instance of the actual bean class '" +
                    targetBeanClass.getName() + "'. If the bean requires proxying " +
                    "(e.g. due to @Transactional), please use class-based proxying.";
            throw new IllegalStateException(getInvocationErrorMessage(targetBean, msg, args));
        }
    }

    private String getInvocationErrorMessage(Object bean, String message, Object[] resolvedArgs) {
        StringBuilder sb = new StringBuilder(getDetailedErrorMessage(bean, message));
        sb.append("Resolved arguments: \n");
        for (int i = 0; i < resolvedArgs.length; i++) {
            sb.append("[").append(i).append("] ");
            if (resolvedArgs[i] == null) {
                sb.append("[null] \n");
            }
            else {
                sb.append("[type=").append(resolvedArgs[i].getClass().getName()).append("] ");
                sb.append("[value=").append(resolvedArgs[i]).append("]\n");
            }
        }
        return sb.toString();
    }

    private String getDetailedErrorMessage(Object bean, String message) {
        StringBuilder sb = new StringBuilder(message).append("\n");
        sb.append("HandlerMethod details: \n");
        sb.append("Bean [").append(bean.getClass().getName()).append("]\n");
        sb.append("Method [").append(this.method.toGenericString()).append("]\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return this.method.toGenericString();
    }
}

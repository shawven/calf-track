package com.github.shawven.calf.track.datasource.api;

import com.github.shawven.calf.track.common.EventAction;
import com.github.shawven.calf.track.datasource.api.event.DataSourceWatchedEvent;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.election.ElectionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基础工作空间
 *
 * @author xw
 * @date 2024/8/9
 */
public abstract class BaseWorkSpace implements ElectionListener {

    private final Logger logger = LoggerFactory.getLogger(BaseWorkSpace.class);

    protected final ApplicationContext applicationContext;

    protected final DataSourceCfg dataSourceCfg;

    private final Map<String, Set<EventAction>> watchedEvents = new ConcurrentHashMap<>();

    public BaseWorkSpace(ApplicationContext applicationContext, DataSourceCfg dataSourceCfg) {
        this.applicationContext = applicationContext;
        this.dataSourceCfg = dataSourceCfg;
    }

    protected boolean filterEvent(EventAction eventAction, String database, String table) {
        String key = database.concat("/").concat(table);
        Set<EventAction> eventActions = watchedEvents.getOrDefault(key, Collections.emptySet());
        return eventActions.contains(eventAction);
    }

    protected void updateWatchedEvents(Collection<ClientInfo> clientInfos) {
        for (ClientInfo client : clientInfos) {
            putEventActions(getEventKey(client), client.getEventActions());

            // 发布数据源关注事件
            logger.info("publishing dataSource watched event");
            applicationContext.publishEvent(
                    new DataSourceWatchedEvent(dataSourceCfg, client.getDbName(), client.getTableName()));
        }
    }

    private String getEventKey(ClientInfo clientInfo) {
        return clientInfo.getDbName().concat("/").concat(clientInfo.getTableName());
    }

    private void putEventActions(String key, Collection<EventAction> actions) {
        watchedEvents.computeIfAbsent(key, (s) -> new HashSet<>()).addAll(actions);
    }
}

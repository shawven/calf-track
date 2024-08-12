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

    private final Map<String, Set<EventAction>> events = new ConcurrentHashMap<>();

    private final Map<String, Map<String, Set<EventAction>>> regexEvents = new ConcurrentHashMap<>();

    public BaseWorkSpace(ApplicationContext applicationContext, DataSourceCfg dataSourceCfg) {
        this.applicationContext = applicationContext;
        this.dataSourceCfg = dataSourceCfg;
    }

    protected boolean filterEvent(EventAction eventAction, String database, String table) {
        String firstKey = database.concat("/").concat(table);
        Set<EventAction> eventActions = events.getOrDefault(firstKey, Collections.emptySet());
        if (eventActions.contains(eventAction)) {
            return true;
        }

        Map<String, Set<EventAction>> map = regexEvents.get(database);
        if (map == null) {
            return false;
        }

        for (Map.Entry<String, Set<EventAction>> entry : map.entrySet()) {
            String key = entry.getKey();
            if (table.matches(key)) {
                return entry.getValue().contains(eventAction);
            }
        }

        return false;
    }

    protected void updateWatchedEvents(Collection<ClientInfo> clientInfos) {
        for (ClientInfo client : clientInfos) {
            putEventActions(client, client.getEventActions());

            // 发布数据源关注事件
            logger.info("publishing dataSource watched event");
            applicationContext.publishEvent(
                    new DataSourceWatchedEvent(dataSourceCfg, client.getDbName(), client.getTableName()));
        }
    }

    private void putEventActions(ClientInfo clientInfo, Collection<EventAction> actions) {
        String dbName = clientInfo.getDbName();
        String tableName = clientInfo.getTableName();

        String firstKey = tableName.concat("/").concat(clientInfo.getTableName());
        events.computeIfAbsent(firstKey, k -> new HashSet<>()).addAll(actions);

        Map<String, Set<EventAction>> map = regexEvents.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>());
        map.computeIfAbsent(tableName, k -> new HashSet<>()).addAll(actions);
    }
}

package com.github.shawven.calf.track.datasource.api;

import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author xw
 * @date 2021/11/15
 */
public abstract class AbstractTrackServer implements TrackServer {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractTrackServer.class);

    protected DataSourceCfgOps dataSourceCfgOps;

    protected ClientOps clientOps;

    protected StatusOps statusOps;

    public AbstractTrackServer(DataSourceCfgOps dataSourceCfgOps,
                               ClientOps clientOps,
                               StatusOps statusOps) {
        this.dataSourceCfgOps = dataSourceCfgOps;
        this.clientOps = clientOps;
        this.statusOps = statusOps;
    }


    @Override
    public void start() {
        // 1. 获得初始配置信息, 根据config开启数据源
        Map<String, List<DataSourceCfg>> namespaceMap = dataSourceCfgOps.getNamespaceMapByType(dataSourceType());
        namespaceMap.forEach((namespace, cfgs) -> {
            if (cfgs.isEmpty()) {
                return;
            }
            for (DataSourceCfg cfg : cfgs) {
                // 在线程中启动事件监听
                if(cfg.isActive()) {
                    doStart(cfg);
                    logger.info("successfully started namespace:{} name:{}", namespace, cfg.getName());
                }
            }
        });

        // 2. 注册数据源Config 命令Watcher
        dataSourceCfgOps.registerServerWatcher(new ServerWatcherImpl());

        // 3. 服务节点上报
        updateServerStatus();
    }

    @Override
    public void stop() {
        // 1. 获得初始配置信息,根据config关闭数据源
        Map<String, List<DataSourceCfg>> namespaceMap = dataSourceCfgOps.getNamespaceMapByType(dataSourceType());
        namespaceMap.forEach((namespace, cfgs) -> {
            if (cfgs.isEmpty()) {
                return;
            }
            for (DataSourceCfg cfg : cfgs) {
                doStop(namespace, cfg.getName());
                logger.info("successfully stopped namespace:{} name:{}", namespace, cfg.getName());
            }
        });
    }

    protected abstract void doStart(DataSourceCfg config);

    protected abstract void doStop(String namespace, String name);

    protected abstract void updateServerStatus();

    private class ServerWatcherImpl implements ServerWatcher {

        @Override
        public void start(Command command) {
            String name = command.getName();
            String namespace = command.getNamespace();
            String delegatedIp = command.getDelegatedIp();
            if(!StringUtils.hasText(name)) {
                return;
            }

            if(StringUtils.hasText(delegatedIp)) {
                CompletableFuture.runAsync(() -> {
                    DataSourceCfg config = dataSourceCfgOps.get(namespace, name);
                    String localIp = getLocalIp(config.getDataSourceType());
                    if(!delegatedIp.equals(localIp)) {
                        logger.info("Ignore command for ip not matching. local: [{}] delegatedId: [{}]", localIp, delegatedIp);
                    }
                });
            }
            CompletableFuture.runAsync(() -> {
                DataSourceCfg config = dataSourceCfgOps.get(namespace, name);
                doStart(config);
                logger.info("command successfully started namespace：{}, name:{}", namespace, name);
            });

        }

        @Override
        public void stop(Command command) {
            String name = command.getName();
            String namespace = command.getNamespace();
            if(!StringUtils.hasText(name)) {
                return;
            }
            CompletableFuture.runAsync(() -> {
                AbstractTrackServer.this.doStop(namespace, name);
                logger.info("command successfully stopped namespace：{}, name:{}", namespace, name);
            });

        }

        private String getLocalIp(String dataSourceType){
            return dataSourceType + ":" + NetUtils.getLocalAddress().getHostAddress();
        }
    }
}


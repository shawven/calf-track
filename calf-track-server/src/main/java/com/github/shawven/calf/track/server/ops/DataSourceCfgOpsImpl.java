package com.github.shawven.calf.track.server.ops;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.ServerWatcher;
import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.register.Emitter;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.Repository;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DataSourceCfgOpsImpl implements DataSourceCfgOps {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceCfgOpsImpl.class);

    private final Repository repository;

    public DataSourceCfgOpsImpl(Repository repository) {
        this.repository = repository;
    }

    @Override
    public Map<String, List<DataSourceCfg>> getNamespaceMapByType(String type) {
        List<DataSourceCfg> dataSourceCfgs = list();

        if (dataSourceCfgs.isEmpty()) {
            logger.warn("There is no available datasource Configs!");
            return Collections.emptyMap();
        }

        return dataSourceCfgs.stream()
                .filter(config -> config.getDataSourceType().equals(type))
                .collect(Collectors.groupingBy(DataSourceCfg::getNamespace));
    }

    @Override
    public boolean create(DataSourceCfg newConfig) {
        if (get(newConfig.getNamespace(), newConfig.getName()) != null) {
            throw new RuntimeException(String.format("namespace: %s exist datasource name: %s",
                    newConfig.getNamespace(), newConfig.getName()));
        }

        if (newConfig.getName() == null) {
            newConfig.setName(System.currentTimeMillis() + "");
        }

        String key = PathKey.concat(Const.DATA_SOURCE, newConfig.getNamespace(), newConfig.getName());
        repository.set(key, JSON.toJSONString(newConfig));
        return true;
    }

    @Override
    public boolean update(DataSourceCfg newConfig) {
        DataSourceCfg dataSourceCfg = get(newConfig.getNamespace(), newConfig.getName());
        if (dataSourceCfg == null) {
            throw new RuntimeException(String.format("namespace: %s not exist datasource name: %s",
                    newConfig.getNamespace(),  newConfig.getName()));
        }
        String key = PathKey.concat(Const.DATA_SOURCE, dataSourceCfg.getNamespace(), dataSourceCfg.getName());
        repository.set(key, JSON.toJSONString(dataSourceCfg));

        return true;
    }

    @Override
    public boolean remove(String namespace, String name) {
        repository.del(PathKey.concat(Const.DATA_SOURCE, namespace, name));
        return true;
    }

    @Override
    public DataSourceCfg get(String namespace, String name) {
        String string = repository.get(PathKey.concat(Const.DATA_SOURCE, namespace, name));
        return JSON.parseObject(string, DataSourceCfg.class);

    }

    @Override
    public List<DataSourceCfg> list() {
        List<String> strings = repository.listTree(PathKey.concat(Const.DATA_SOURCE));
        return strings.stream()
                .map(s -> JSON.parseObject(s, DataSourceCfg.class))
                .collect(Collectors.toList());
    }

    @Override
    public List<DataSourceCfg> list(String namespace) {
        List<String> strings = repository.listTree(PathKey.concat(Const.DATA_SOURCE, namespace));
        return strings.stream()
                .map(s -> JSON.parseObject(s, DataSourceCfg.class))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listNames(String namespace) {
        return list(namespace).stream()
                .map(DataSourceCfg::getName)
                .collect(Collectors.toList());
    }

    @Override
    public void registerServerWatcher(ServerWatcher watcher) {
        repository.watch(PathKey.concat(Const.COMMAND), new Emitter<String>() {
            @Override
            public void onNext(String value) {
                Command command = JSON.parseObject(value, Command.class);
                // 根据不同的命令类型（START/STOP）执行不同的逻辑
                if(Command.Type.START.equals(command.getType())) {
                    watcher.start(command);
                } else if (Command.Type.STOP.equals(command.getType())) {
                    watcher.stop(command);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("watch command error.", throwable);
                CompletableFuture.runAsync(() ->  registerServerWatcher(watcher));
            }

            @Override
            public void onComplete() {
                logger.info("watch command completed.");
                CompletableFuture.runAsync(() ->  registerServerWatcher(watcher));
            }
        });
    }
}

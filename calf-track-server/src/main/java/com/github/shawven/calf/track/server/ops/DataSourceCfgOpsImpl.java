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
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DataSourceCfgOpsImpl implements DataSourceCfgOps {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceCfgOpsImpl.class);

    private final Repository repository;

    public DataSourceCfgOpsImpl(Repository repository) {
        this.repository = repository;
    }

    @Override
    public List<DataSourceCfg> getByDataSourceType(String type) {
        List<DataSourceCfg> dataSourceCfgs = listCfgs();

        if(dataSourceCfgs.isEmpty()) {
            logger.warn("There is no available datasource Configs!");
            return dataSourceCfgs;
        }
        Set<String> namespaces = new HashSet<>();
        List<DataSourceCfg> filterDataSource = new ArrayList<>();
        dataSourceCfgs.forEach(config -> {
            if(!StringUtils.hasText(config.getNamespace())) {
                throw new IllegalArgumentException("You need to config namespace!");
            }
            if(!namespaces.add(config.getNamespace())) {
                throw new IllegalArgumentException("Duplicated namespace!");
            }
            if(config.getDataSourceType().equals(type)){
                filterDataSource.add(config);
            }
        });
        return filterDataSource;
    }

    @Override
    public boolean create(DataSourceCfg newConfig) {
        List<DataSourceCfg> dataSourceCfgs = listCfgs();

        boolean exist = dataSourceCfgs.stream().anyMatch(c -> Objects.equals(newConfig.getId(), c.getId()));
        if(exist) {
            return false;
        }

        newConfig.setId(UUID.randomUUID().toString().replace("-", ""));
        if (dataSourceCfgs.isEmpty()) {
            dataSourceCfgs = Collections.singletonList(newConfig);
        } else {
            dataSourceCfgs.add(newConfig);
        }

        saveAll(dataSourceCfgs);
        return true;
    }

    @Override
    public boolean update(DataSourceCfg newConfig) {
        if(Thread.currentThread().isInterrupted()) {
            return false;
        }
        List<DataSourceCfg> configList = listCfgs();

        AtomicBoolean modifyFlag = new AtomicBoolean(false);
        configList = configList.stream()
                .map((c) -> {
                    if(Objects.equals(newConfig.getId(), c.getId())) {
                        // 版本号小于集群中版本号则忽略
                        if(newConfig.getVersion() < c.getVersion()) {
                            logger.warn("Ignore BinLogConfig[{}] Modify case local version [{}] < current version [{}]", newConfig.getNamespace(), newConfig.getVersion(), c.getVersion());
                            return c;
                        } else {
                            modifyFlag.set(true);
                            return newConfig;
                        }
                    }
                    return c;
                }).collect(Collectors.toList());

        if(modifyFlag.get()) {
            saveAll(configList);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(String id) {
        if(!StringUtils.hasText(id)) {
            return false;
        }

        List<DataSourceCfg> dataSourceCfgs = listCfgs();
        dataSourceCfgs.removeIf(c -> Objects.equals(id, c.getId()));

        saveAll(dataSourceCfgs);

        return true;
    }

    @Override
    public DataSourceCfg getByNamespace(String namespace) {
        List<DataSourceCfg> dataSourceCfgs = listCfgs();
        Optional<DataSourceCfg> optional = dataSourceCfgs.stream()
                .filter(config -> namespace.equals(config.getNamespace()))
                .findAny();

        return optional.orElse(null);

    }
    @Override
    public List<String> getNamespaceList() {
        return listCfgs().stream()
                .map(DataSourceCfg::getNamespace)
                .collect(Collectors.toList());
    }


    @Override
    public List<DataSourceCfg> listCfgs() {
        String value = repository.get(PathKey.concat(Const.NODE_CONFIG));
        if (value == null) {
            return Collections.emptyList();
        }
        return JSON.parseArray(value, DataSourceCfg.class);
    }

    private void saveAll(List<DataSourceCfg> dataSourceCfgs) {
        repository.set(PathKey.concat(Const.NODE_CONFIG), JSON.toJSONString(dataSourceCfgs));
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

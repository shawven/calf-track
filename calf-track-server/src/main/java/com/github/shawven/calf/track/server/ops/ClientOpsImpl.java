package com.github.shawven.calf.track.server.ops;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.register.Emitter;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.Repository;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author xw
 * @date 2023-01-05
 */
public class ClientOpsImpl implements ClientOps {

    private static final Logger logger = LoggerFactory.getLogger(ClientOpsImpl.class);

    private final Repository repository;

    private final DataSourceCfgOps dataSourceCfgOps;

    public ClientOpsImpl(Repository repository, DataSourceCfgOps dataSourceCfgOps) {
        this.repository = repository;
        this.dataSourceCfgOps = dataSourceCfgOps;
    }

    @Override
    public List<ClientInfo> listConsumerClientsByNamespaceAndName(String namespace, String name) {
        List<String> strings = repository.listTree(clientKey(namespace));
        return strings.stream()
                .map(str -> JSON.parseObject(str, ClientInfo.class))
                .filter(clientInfo -> Objects.equals(clientInfo.getDsName(), name))
                .collect(Collectors.toList());
    }

    @Override
    public List<ClientInfo> listConsumerClientsByNamespaceAndQueueType(String namespace, String queueType) {
        return  repository.listTree(clientKey(namespace)).stream()
                .map(str -> JSON.parseObject(str, ClientInfo.class))
                .filter(clientInfo -> queueType == null || queueType.equals(clientInfo.getQueueType()))
                .collect(Collectors.toList());
    }

    @Override
    public List<ClientInfo> listConsumerClientsByKey(String namespace, String key) {
        return repository.listTree(clientKey(namespace)).stream()
                .map(str -> JSON.parseObject(str, ClientInfo.class))
                .filter(clientInfo -> Objects.equals(key, clientInfo.getKey()))
                .collect(Collectors.toList());
    }

    @Override
    public void addConsumerClient(ClientInfo clientInfo) {
        String namespace = clientInfo.getNamespace();
        if (!dataSourceCfgOps.listNames(namespace).contains(clientInfo.getDsName())) {
            throw new RuntimeException(String.format("namespace: %s not exist dataSource: %s",
                    namespace, clientInfo.getDsName()));
        }

        String name = clientInfo.getName();
        if (name == null) {
            clientInfo.setName(System.currentTimeMillis() + "");
        } else if (repository.get(clientKey(namespace, name)) != null) {
            throw new RuntimeException(String.format("namespace: %s exist client : %s",
                    namespace, name));
        }

        repository.set(clientKey(namespace, name), JSON.toJSONString(clientInfo));

        logger.info("addConsumerClient success namespace:{} client: {}", namespace, clientInfo);
    }

    @Override
    public void removeConsumerClient(List<ClientInfo> clientInfos) {
        String namespace = clientInfos.iterator().next().getNamespace();
        for (ClientInfo clientInfo : clientInfos) {
            repository.del(clientKey(namespace, clientInfo.getName()));
        }
        logger.info("removeConsumerClient success namespace:{} clients: {}", namespace, clientInfos);
    }

    @Override
    public boolean sendCommand(Command command) {
        repository.set(PathKey.concat(Const.COMMAND), JSON.toJSONString(command));
        return true;
    }


    @Override
    public void watcherClientInfo(DataSourceCfg cfg, Consumer<List<ClientInfo>> consumer) {
        repository.watch(clientKey(cfg.getNamespace()), new Emitter<String>() {
            @Override
            public void onNext(String value) {
                List<ClientInfo> list = JSON.parseArray(value, ClientInfo.class).stream()
                        .filter(clientInfo -> Objects.equals(clientInfo.getDsName(), cfg.getName()))
                        .collect(Collectors.toList());
                consumer.accept(list);
            }

            @Override
            public void onError(Throwable e) {
                logger.error("watcherClientInfos.onError: " + e.getMessage(), e);
            }

            @Override
            public void onComplete() {
                logger.info("watcherClientInfos.onCompleted");
            }
        });
    }

    private String clientKey(String namespace) {
        return PathKey.concat(Const.CLIENT_SET_KEY, namespace);
    }

    private String clientKey(String namespace, String name) {
        return PathKey.concat(Const.CLIENT_SET_KEY, namespace, name);
    }
}

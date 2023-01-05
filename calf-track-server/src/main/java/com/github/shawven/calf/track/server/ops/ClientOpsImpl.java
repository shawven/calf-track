package com.github.shawven.calf.track.server.ops;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.register.Emitter;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.*;
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
    public List<ClientInfo> listConsumerClient(DataSourceCfg dataSourceCfg) {
        String namespace = dataSourceCfg.getNamespace();
        String binLogClientSetStr = repository.get(joinKey(namespace));

        if(!StringUtils.hasText(binLogClientSetStr)) {
            return new ArrayList<>();
        }
        return JSON.parseArray(binLogClientSetStr, ClientInfo.class);
    }

    @Override
    public List<ClientInfo> listConsumerClient(String queryType) {
        return dataSourceCfgOps.listCfgs().stream()
                .map(config -> repository.get(joinKey(config.getNamespace())))
                .map(str -> {
                    List<ClientInfo> clientInfos = JSON.parseArray(str, ClientInfo.class);
                    return clientInfos == null ? new ArrayList<ClientInfo>() : clientInfos;
                })
                .flatMap(List::stream)
                .filter(clientInfo -> queryType == null || queryType.equals(clientInfo.getQueueType()))
                .sorted(Comparator.comparing(ClientInfo::getNamespace))
                .collect(Collectors.toList());
    }

    @Override
    public List<ClientInfo> listConsumerClientsByKey(String clientInfoKey) {
        List<ClientInfo> clientInfos = listConsumerClient((String) null);
        if(clientInfos == null || clientInfos.isEmpty()) {
            return clientInfos;
        }

        return clientInfos
                .stream()
                .filter(clientInfo -> clientInfo.getKey().equals(clientInfoKey))
                .collect(Collectors.toList());
    }

    @Override
    public void addConsumerClient(ClientInfo clientInfo) {
        String namespace = clientInfo.getNamespace();
        DataSourceCfg config = dataSourceCfgOps.getByNamespace(namespace);
        if (config == null) {
            throw new RuntimeException("not exist namespace: " + namespace);
        }
        String metaData = repository.get(joinKey(namespace));

        Set<ClientInfo> clientSet = null;
        if(!StringUtils.hasText(metaData)) {
            clientSet = new HashSet<>();
        } else {
            List<ClientInfo> clientList = JSON.parseArray(metaData, ClientInfo.class);
            clientSet = new HashSet<>(clientList);
        }
        clientSet.add(clientInfo);
        repository.set(joinKey(namespace), JSON.toJSONString(clientSet));

        logger.info("addConsumerClient success namespace:{} client: {}", namespace, clientInfo);
    }

    @Override
    public void removeConsumerClient(List<ClientInfo> clientInfos) {
        Map<String, List<ClientInfo>> clientMap = clientInfos.stream().collect(Collectors.groupingBy(ClientInfo::getNamespace));
        clientMap.forEach((namespace, clientList) -> {

            DataSourceCfg config = dataSourceCfgOps.getByNamespace(namespace);
            String metaData = repository.get(joinKey(namespace));
            if(StringUtils.hasText(metaData)) {
                List<ClientInfo> currentList = JSON.parseArray(metaData, ClientInfo.class);
                Set<ClientInfo> clientSet = new HashSet<>(currentList);
                clientList.forEach(clientSet::remove);
                repository.set(joinKey(namespace), JSON.toJSONString(clientSet));

                logger.info("removeConsumerClient success namespace:{} clients: {}", namespace, currentList);
            }
        });
    }

    @Override
    public boolean sendCommand(Command command) {
        repository.set(PathKey.concat(Const.COMMAND), JSON.toJSONString(command));
        return true;
    }


    @Override
    public void watcherClientInfo(DataSourceCfg dataSourceCfg, Consumer<List<ClientInfo>> consumer) {
        repository.watch(joinKey(dataSourceCfg.getNamespace()), new Emitter<String>() {
            @Override
            public void onNext(String value) {
                List<ClientInfo> clientInfos = JSON.parseArray(value, ClientInfo.class);
                consumer.accept(clientInfos);
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

    private String joinKey(String namespace) {
        return PathKey.concat(namespace, Const.CLIENT_SET_KEY);
    }
}

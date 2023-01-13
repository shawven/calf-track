package com.github.shawven.calf.track.datasource.api.ops;

import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface ClientOps {

    List<ClientInfo> listConsumerClientsByNamespaceAndName(String namespace, String name);

    List<ClientInfo> listConsumerClientsByNamespaceAndQueueType(String namespace, String queueType);

    List<ClientInfo> listConsumerClientsByKey(String namespace, String clientInfoKey);

    void addConsumerClient(ClientInfo clientInfo);

    void removeConsumerClient(List<ClientInfo> clientInfos);

    boolean sendCommand(Command command);

    void watcherClientInfo(DataSourceCfg dataSourceCfg, Consumer<List<ClientInfo>> consumer);
}

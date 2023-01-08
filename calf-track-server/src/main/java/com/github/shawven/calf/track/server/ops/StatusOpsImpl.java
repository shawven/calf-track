package com.github.shawven.calf.track.server.ops;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.DataSourceStatus;
import com.github.shawven.calf.track.register.domain.ServerStatus;
import com.github.shawven.calf.track.register.Repository;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class StatusOpsImpl implements StatusOps {

    private final Repository repository;

    private final DataSourceCfgOps dataSourceCfgOps;

    public StatusOpsImpl(Repository repository, DataSourceCfgOps dataSourceCfgOps) {
        this.repository = repository;
        this.dataSourceCfgOps = dataSourceCfgOps;
    }

    @Override
    public void updateDataSourceStatus(String filename, long position, DataSourceCfg config) {
        long now = System.currentTimeMillis();
        String dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault())
                .format(DateTimeFormatter.ISO_DATE_TIME);

        String name = config.getName();
        String namespace = config.getNamespace();

        String metaData = repository.get(getKey(namespace, name));
        DataSourceStatus status;
        if(!StringUtils.hasText(metaData)) {
            status = new DataSourceStatus();
            status.setName(config.getName());
            status.setNamespace(config.getNamespace());
        } else {
            status = JSON.parseObject(metaData).toJavaObject(DataSourceStatus.class);
        }

        status.setFilename(filename);
        status.setPosition(position);
        status.setTimestamp(now);
        status.setDateTime(dateTime);

        repository.set(getKey(namespace, name), JSON.toJSONString(status));
    }

    @Override
    public List<DataSourceStatus> listStatus(String namespace) {
        return dataSourceCfgOps.list(namespace).stream()
                .map(config -> {
                    String metaData = repository.get(getKey(config.getNamespace(), config.getName()));
                    if (metaData == null) {
                        return null;
                    }
                    // metaData shouldn't be null
                    return JSON.parseObject(metaData).toJavaObject(DataSourceStatus.class);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public DataSourceStatus getDataSourceStatus(DataSourceCfg config) {
        String name = config.getName();
        String namespace = config.getNamespace();

        String metaData = repository.get(getKey(namespace, name));
        if(!StringUtils.hasText(metaData)) {
            return null;
        }
        return JSON.parseObject(metaData, DataSourceStatus.class);
    }


    @Override
    public void updateServerStatus(String serverKey, ServerStatus status) {
        repository.set(
                PathKey.concat(Const.SERVER_STATUS, serverKey),
                JSON.toJSONString(status),
                20
        );
    }

    /**
     * @return
     */
    @Override
    public List<ServerStatus> getServerStatus() {
        List<String> strings = repository.listTree(PathKey.concat(Const.SERVER_STATUS));
        return strings.stream()
                .map(str -> JSON.parseObject(str, ServerStatus.class))
                .collect(Collectors.toList());
    }

    private String getKey(String namespace, String name) {
        return PathKey.concat(Const.STATUS_KEY, namespace, name);
    }
}

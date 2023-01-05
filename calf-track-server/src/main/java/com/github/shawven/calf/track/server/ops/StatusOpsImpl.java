package com.github.shawven.calf.track.server.ops;

import com.alibaba.fastjson.JSON;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.PathKey;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.DataSourceStatus;
import com.github.shawven.calf.track.register.domain.InstanceStatus;
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

        String namespace = config.getNamespace();

        String metaData = repository.get(getKey(namespace));
        DataSourceStatus status;
        if(!StringUtils.hasText(metaData)) {
            status = new DataSourceStatus();
            status.setNamespace(config.getNamespace());
        } else {
            status = JSON.parseObject(metaData).toJavaObject(DataSourceStatus.class);
        }

        status.setFilename(filename);
        status.setPosition(position);
        status.setTimestamp(now);
        status.setDateTime(dateTime);

        repository.set(getKey(namespace), JSON.toJSONString(status));
    }

    @Override
    public List<DataSourceStatus> listStatus() {
        return dataSourceCfgOps.listCfgs().stream()
                .map(config -> {
                    String metaData = repository.get(getKey(config.getNamespace()));
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
    public DataSourceStatus getDataSourceStatus(DataSourceCfg dataSourceCfg) {
        String namespace = dataSourceCfg.getNamespace();

        String metaData = repository.get(getKey(namespace));
        if(!StringUtils.hasText(metaData)) {
            return null;
        }
        return JSON.parseObject(metaData, DataSourceStatus.class);
    }


    @Override
    public void updateInstanceStatus(String serviceKey, InstanceStatus status) {
        repository.set(
                PathKey.concat(Const.SERVICE_STATUS_PATH, serviceKey),
                JSON.toJSONString(status),
                20
        );
    }

    /**
     * @return
     */
    @Override
    public List<InstanceStatus> getInstanceStatus() {
        List<String> strings = repository.listChildren(PathKey.concat(Const.SERVICE_STATUS_PATH));
        return strings.stream()
                .map(str -> JSON.parseObject(str, InstanceStatus.class))
                .collect(Collectors.toList());
    }

    private String getKey(String namespace) {
        return PathKey.concat(namespace, Const.STATUS_KEY);
    }
}

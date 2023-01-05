package com.github.shawven.calf.track.datasource.api.ops;


import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.DataSourceStatus;
import com.github.shawven.calf.track.register.domain.InstanceStatus;

import java.util.List;

public interface StatusOps {

    List<DataSourceStatus> listStatus();

    void updateDataSourceStatus(String filename, long position, DataSourceCfg dataSourceCfg);

    DataSourceStatus getDataSourceStatus(DataSourceCfg dataSourceCfg);

    void updateInstanceStatus(String serviceKey, InstanceStatus status);

    List<InstanceStatus> getInstanceStatus();
}

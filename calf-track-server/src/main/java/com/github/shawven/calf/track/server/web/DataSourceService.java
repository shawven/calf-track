package com.github.shawven.calf.track.server.web;

import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.InstanceStatus;

import java.util.List;

public interface DataSourceService {

    List<DataSourceCfg> listCfgs();

    List<InstanceStatus> getServiceStatus();

    boolean saveDatasourceConfig(DataSourceCfg config);

    boolean updateDatasourceConfig(DataSourceCfg config);

    boolean removeDatasourceConfig(String namespace);

    /**
     * 发送开启数据源命令
     *
     * @param namespace
     * @param delegatedIp
     * @return
     */
    boolean startDatasource(String namespace, String delegatedIp);

    /**
     * 发送关闭数据源命令
     *
     * @param namespace
     * @return
     */
    boolean stopDatasource(String namespace);
}

package com.github.shawven.calf.track.server.web;

import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.ServerStatus;

import java.util.List;

public interface DataSourceService {

    List<ServerStatus> getServiceStatus();

    List<DataSourceCfg> listCfgs(String namespace);
    /**
     * 获取所有的namespace
     */
    List<String> listNames(String namespace);

    boolean saveDatasourceConfig(DataSourceCfg config);

    boolean updateDatasourceConfig(DataSourceCfg config);

    boolean removeDatasourceConfig(String namespace, String name);

    /**
     * 发送开启数据源命令
     *
     * @param namespace
     * @param name
     * @param ip
     * @return
     */
    boolean startDatasource(String namespace, String name, String ip);

    /**
     * 发送关闭数据源命令
     *
     * @param namespace
     * @return
     */
    boolean stopDatasource(String namespace, String name);
}

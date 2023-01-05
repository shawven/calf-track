package com.github.shawven.calf.track.datasource.api.ops;

import com.github.shawven.calf.track.datasource.api.ServerWatcher;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;

import java.util.List;

/**
 * @author xw
 * @date 2021/11/15
 */
public interface DataSourceCfgOps {

    List<DataSourceCfg> getByDataSourceType(String type);

    boolean create(DataSourceCfg config);

    boolean update(DataSourceCfg config);

    boolean remove(String namespace);

    DataSourceCfg getByNamespace(String namespace);

    List<DataSourceCfg> listCfgs();

    List<String> getNamespaceList();

    void registerServerWatcher(ServerWatcher watcher);
}

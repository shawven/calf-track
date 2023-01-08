package com.github.shawven.calf.track.datasource.api.ops;

import com.github.shawven.calf.track.datasource.api.ServerWatcher;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;

import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2021/11/15
 */
public interface DataSourceCfgOps {

    Map<String, List<DataSourceCfg>> getNamespaceMapByType(String type);

    boolean create(DataSourceCfg config);

    boolean update(DataSourceCfg config);

    boolean remove(String namespace, String name);

    List<DataSourceCfg> list();

    List<DataSourceCfg> list(String namespace);

    List<String> listNames(String namespace);

    DataSourceCfg get(String namespace, String name);

    void registerServerWatcher(ServerWatcher watcher);
}

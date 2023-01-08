package com.github.shawven.calf.track.server.web;

import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.ServerStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataSourceServiceImpl implements DataSourceService {

    @Autowired
    private DataSourceCfgOps dataSourceCfgOps;

    @Autowired
    private StatusOps statusOps;

    @Autowired
    private ClientOps clientOps;

    @Override
    public List<ServerStatus> getServiceStatus() {
        return statusOps.getServerStatus();
    }

    @Override
    public List<DataSourceCfg> listCfgs(String namespace) {
        return dataSourceCfgOps.list(namespace);
    }

    @Override
    public List<String> listNames(String namespace) {
        return dataSourceCfgOps.listNames(namespace);
    }

    @Override
    public boolean saveDatasourceConfig(DataSourceCfg config) {
        return dataSourceCfgOps.create(config);
    }

    @Override
    public boolean updateDatasourceConfig(DataSourceCfg config) {
        return dataSourceCfgOps.update(config);
    }

    @Override
    public boolean removeDatasourceConfig(String namespace, String name) {
        return dataSourceCfgOps.remove(namespace, name);
    }


    @Override
    public boolean startDatasource(String namespace, String name, String ip) {
        Command command = new Command(namespace, name, Command.Type.START);
        return clientOps.sendCommand(command);
    }


    @Override
    public boolean stopDatasource(String namespace, String name) {
        Command command = new Command(namespace, name, Command.Type.STOP);
        return clientOps.sendCommand(command);
    }
}

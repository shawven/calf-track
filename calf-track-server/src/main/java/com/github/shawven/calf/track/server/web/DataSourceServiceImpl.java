package com.github.shawven.calf.track.server.web;

import com.github.shawven.calf.track.datasource.api.domain.Command;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.DataSourceCfgOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.InstanceStatus;
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
    public List<DataSourceCfg> listCfgs() {
        return dataSourceCfgOps.listCfgs();
    }

    @Override
    public List<InstanceStatus> getServiceStatus() {
        return statusOps.getInstanceStatus();
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
    public boolean removeDatasourceConfig(String namespace) {
        return dataSourceCfgOps.remove(namespace);
    }


    @Override
    public boolean startDatasource(String namespace, String delegatedIp) {
        Command command = new Command(namespace, delegatedIp, Command.Type.START);
        return clientOps.sendCommand(command);
    }


    @Override
    public boolean stopDatasource(String namespace) {
        Command command = new Command(namespace, Command.Type.STOP);
        return clientOps.sendCommand(command);
    }
}

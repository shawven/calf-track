package com.github.shawven.calf.track.server.web;

import com.alibaba.fastjson.JSONObject;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import com.github.shawven.calf.track.register.domain.ServerStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
@RestController
@RequestMapping("/datasource")
public class DataSourceController {

    @Autowired
    private DataSourceService dataSourceService;

    @GetMapping("/list")
    public List<DataSourceCfg> datasourceConfigs(String namespace) {
        return dataSourceService.listCfgs(namespace);
    }

    @PostMapping("/save")
    public Result saveDatasourceConfig(String namespace, @RequestBody DataSourceCfg config) {
        config.setNamespace(namespace);
        if(dataSourceService.saveDatasourceConfig(config)) {
            return new Result(Result.SUCCESS, "添加数据源成功");
        } else {
            return new Result(Result.ERROR, "添加数据源失败，命名空间已存在");
        }
    }

    @PostMapping("/update")
    public Result updateDatasourceConfig(String namespace, @RequestBody DataSourceCfg config) {
        config.setNamespace(namespace);
        if(dataSourceService.updateDatasourceConfig(config)) {
            return new Result(Result.SUCCESS, "更新数据源成功");
        } else {
            return new Result(Result.ERROR, "更新数据源失败");
        }
    }

    @PostMapping("/remove")
    public Result removeDatasourceConfig(String namespace, @RequestBody DataSourceCfg config) {
        if(dataSourceService.removeDatasourceConfig(namespace, config.getName())) {
            return new Result(Result.SUCCESS, "移除数据源成功");
        } else {
            return new Result(Result.ERROR, "移除数据源失败");
        }
    }

    @PostMapping("/start")
    public Result startDatasource(String namespace, @RequestBody JSONObject jsonObject) {
        String name = jsonObject.getString("name");
        String delegatedIp = jsonObject.getString("delegatedIp");

        if(dataSourceService.startDatasource(namespace, name, delegatedIp)) {
            return new Result(Result.SUCCESS, "发送开启数据源监听命令成功");
        } else {
            return new Result(Result.ERROR, "发送开启数据源监听命令失败");
        }
    }

    @PostMapping("/stop")
    public Result stopDatasource(String namespace, @RequestBody JSONObject jsonObject) {
        String name = jsonObject.getString("name");
        if (dataSourceService.stopDatasource(namespace, name)) {
            return new Result(Result.SUCCESS, "发送关闭数据源监听命令成功");
        } else {
            return new Result(Result.ERROR, "发送关闭数据源监听命令失败");
        }
    }

    @GetMapping("/service-status")
    public List<ServerStatus> getServiceStatus() {
        return dataSourceService.getServiceStatus();
    }

    @GetMapping("/names")
    public List<String> namespaceList(String namespace) {
        return dataSourceService.listNames(namespace);
    }
}

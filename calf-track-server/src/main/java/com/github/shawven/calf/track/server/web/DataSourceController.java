package com.github.shawven.calf.track.server.web;

import com.alibaba.fastjson.JSONObject;
import com.github.shawven.calf.track.register.domain.InstanceStatus;
import com.github.shawven.calf.track.register.domain.DataSourceCfg;
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
    public List<DataSourceCfg> datasourceConfigs() {
        return dataSourceService.listCfgs();
    }

    @PostMapping("/persist")
    public Result saveDatasourceConfig(@RequestBody DataSourceCfg config) {
        if(dataSourceService.saveDatasourceConfig(config)) {
            return new Result(Result.SUCCESS, "添加数据源成功");
        } else {
            return new Result(Result.ERROR, "添加数据源失败，命名空间已存在");
        }
    }

    @PostMapping("/update")
    public Result updateDatasourceConfig(@RequestBody DataSourceCfg config) {
        if(dataSourceService.updateDatasourceConfig(config)) {
            return new Result(Result.SUCCESS, "更新数据源成功");
        } else {
            return new Result(Result.ERROR, "更新数据源失败");
        }
    }

    @PostMapping("/remove")
    public Result removeDatasourceConfig(@RequestBody DataSourceCfg config) {
        if(dataSourceService.removeDatasourceConfig(config.getNamespace())) {
            return new Result(Result.SUCCESS, "移除数据源成功");
        } else {
            return new Result(Result.ERROR, "移除数据源失败");
        }
    }

    @PostMapping("/start")
    public Result startDatasource(@RequestBody JSONObject jsonObject) {
        String namespace = jsonObject.getString("namespace");
        String delegatedIp = jsonObject.getString("delegatedIp");

        if(dataSourceService.startDatasource(namespace, delegatedIp)) {
            return new Result(Result.SUCCESS, "发送开启数据源监听命令成功");
        } else {
            return new Result(Result.ERROR, "发送开启数据源监听命令失败");
        }
    }

    @PostMapping("/stop")
    public Result stopDatasource(@RequestBody JSONObject JsonObject) {

        if(dataSourceService.stopDatasource(JsonObject.getString("namespace"))) {
            return new Result(Result.SUCCESS, "发送关闭数据源监听命令成功");
        } else {
            return new Result(Result.ERROR, "发送关闭数据源监听命令失败");
        }
    }

    @GetMapping("/service-status")
    public List<InstanceStatus> getServiceStatus() {
        return dataSourceService.getServiceStatus();
    }
}

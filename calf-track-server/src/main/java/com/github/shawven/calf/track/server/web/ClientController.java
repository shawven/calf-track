package com.github.shawven.calf.track.server.web;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

import static org.springframework.web.bind.annotation.RequestMethod.*;

/**
 * @author xw
 * @date 2023-01-05
 */
@RestController
@RequestMapping("/client")
public class ClientController {
    private static final Logger log = LoggerFactory.getLogger(ClientController.class);

    @Autowired
    ClientService clientService;

    @PostMapping(value = "/add")
    public Result add(@RequestBody String data) {
        JSONObject jsonObject = JSON.parseObject(data);
        JSONArray eventActions = jsonObject.getJSONArray("eventAction");
        Integer partitions = jsonObject.getInteger("partitions");
        Integer replication = jsonObject.getInteger("replication");
        for (int i = 0; i < eventActions.size(); i++) {
            JSONObject object = JSON.parseObject(data);
            object.put("eventAction", eventActions.getString(i));
            ClientInfo clientInfo = JSON.parseObject(JSON.toJSONString(object), ClientInfo.class);
            //生成key字符串
            clientInfo = new ClientInfo(
                    clientInfo.getClientId(),
                    clientInfo.getQueueType(),
                    clientInfo.getNamespace(),
                    clientInfo.getDatabaseName(),
                    clientInfo.getTableName(), clientInfo.getEventAction());

            clientService.addClient(clientInfo, partitions, replication);
        }
        return Result.success("添加成功");
    }

    @RequestMapping(value = "/addAll", method = POST)
    public Result addAll(@RequestBody String data) {
        log.info(data);
        List<ClientInfo> clientInfos = JSON.parseArray(data, ClientInfo.class);
        clientInfos.forEach(clientInfo -> clientService.addClient(clientInfo, null, null));
        return Result.success("添加成功");
    }

    /**
     * 列出所有队列
     * @return
     */
    @RequestMapping(value = "/listClientMap", method = GET)
    public Map<String, List<ClientInfo>> listClientMap() {
        return clientService.listClientMap();
    }

    /**
     * 列出所有队列
     * @return
     */
    @RequestMapping(value = "/list", method = GET)
    public List<ClientInfo> list() {
        return clientService.listClient(null);
    }

    /**
     * 列出Redis队列
     * @return
     */
    @RequestMapping(value = "/listRedis", method = GET)
    public List<ClientInfo> listRedis() {
        return clientService.listClient(Const.QUEUE_TYPE_REDIS);
    }

    /**
     * 列出Rabbit队列
     * @return
     */
    @RequestMapping(value = "/listRabbit", method = GET)
    public List<ClientInfo> listRabbit() {
        return clientService.listClient(Const.QUEUE_TYPE_RABBIT);
    }

    /**
     * 列出Kafka队列
     * @return
     */
    @RequestMapping(value = "/listKafka", method = GET)
    public List<ClientInfo> listKafka() {
        return clientService.listClient(Const.QUEUE_TYPE_KAFKA);
    }

    /**
     * 列出异常队列
     * @return
     */
    @RequestMapping(value = "/listErr",method = GET)
    public List<String> listErr(){
        return clientService.listErrorClient();
    }

    @RequestMapping(value = "/delete", method = POST)
    public Result delete(@RequestBody ClientInfo clientInfo) {
        clientService.deleteClient(clientInfo);
        return Result.success("删除成功!");
    }

    /**
     * 获取队列长度
     *
     * @return
     */
    @RequestMapping(value = "/getqueuesize", method = GET)
    public String getQueueSize(String clientName,String type,int page) {
        return clientService.getqueuesize(clientName,type,page);
    }
    /**
     * 获取日志文件状态
     *
     * @return
     */
    @RequestMapping(value = "/getlogstatus", method = GET)
    public String getLogStatus() {
        return clientService.getLogStatus();
    }

    /**
     * 错误队列重新入队
     * @param uuid uuid
     * @param errClient 队列名
     * @return
     */
    @RequestMapping(value = "/deleteFromQueue",method =GET)
    public boolean enqueueAgain(String uuid,String errClient){
        return clientService.deleteFromQueue(uuid,errClient);
    }

    @GetMapping("/namespaceList")
    public List<String> namespaceList() {
        return clientService.listNamespace();
    }

    @GetMapping("/deleteTopic")
    public Result deleteTopic(String clientInfoKey) {
        return clientService.deleteTopic(clientInfoKey);
    }
}


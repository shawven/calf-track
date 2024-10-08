package com.github.shawven.calf.track.server.web;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.domain.DataErrorMsg;
import com.github.shawven.calf.track.datasource.api.ops.ClientOps;
import com.github.shawven.calf.track.datasource.api.ops.StatusOps;
import com.github.shawven.calf.track.register.domain.ClientInfo;
import com.github.shawven.calf.track.server.publisher.kafka.KafkaService;
import com.github.shawven.calf.track.server.publisher.rabbit.RabbitService;
import com.rabbitmq.http.client.domain.QueueInfo;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author xw
 * @date 2023-01-05
 */
@Service
public class ClientServiceImpl implements ClientService {

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private RabbitService rabbitService;

    @Autowired
    private KafkaService kafkaService;

    @Autowired
    private ClientOps clientOps;

    @Autowired
    private StatusOps statusOps;

    @Autowired
    private DataPublisher dataPublisher;



    @Override
    public void saveClient(ClientInfo clientInfo) {
        clientOps.saveConsumerClient(clientInfo);
    }

    @Override
    public List<ClientInfo> listClient(String namespace, String queryType) {
        return clientOps.listConsumerClientsByNamespaceAndQueueType(namespace, queryType);
    }

    @Override
    public Map<String, List<ClientInfo>> listClientMap(String namespace) {
        List<ClientInfo> clientInfos = listClient(namespace, null);
        if (clientInfos == null || clientInfos.isEmpty()) {
            return new HashMap<>();
        } else {
            return clientInfos.stream().collect(Collectors.groupingBy(ClientInfo::getKey));
        }
    }

    @Override
    public List<String> listErrorClient(String namespace) {

        List<String> clientList = new ArrayList();
        Iterable<String> keys = redissonClient.getKeys().getKeysByPattern(Const.REDIS_PREFIX.concat("BIN-LOG-ERR-MAP-*"));
        keys.forEach(key -> {
            clientList.add(key);
        });
        return clientList;
    }

    @Override
    public void deleteClient(ClientInfo clientInfo) {
        clientOps.removeConsumerClient(Collections.singletonList(clientInfo));
    }

    @Override
    public String getLogStatus(String namespace) {
        return JSONArray.toJSONString(statusOps.listStatus(namespace));
    }

    @Override
    public String getqueuesize(String namespace, String clientName, String type, int page) {
        int repage = 10 * (page - 1);

        JSONObject object = new JSONObject();
        String ClientId;
        if (Const.QUEUE_TYPE_REDIS.equals(type)) {
            ClientId = "BIN-LOG-DATA-" + namespace + "-".concat(clientName);
            object.put("queueSize", redissonClient.getList(ClientId).size());
            object.put("queue", redissonClient.getList(ClientId)
                    .get(repage, repage + 1, repage + 2, repage + 3, repage + 4, repage + 5, repage + 6, repage + 7, repage + 8, repage + 9));
            return object.toJSONString();
        } else if (Const.QUEUE_TYPE_RABBIT.equals(type)) {
            ClientId = "BIN-LOG-DATA-" + namespace + "-".concat(clientName);
            QueueInfo queueInfo = rabbitService.getQueue(ClientId);
            if (queueInfo == null || 0 == queueInfo.getMessagesReady()) {
                object.put("queueSize", 0);
                object.put("queue", new ArrayList<>());
                return object.toJSONString();
            } else {
                long queueSize = queueInfo.getMessagesReady();
                long count = (repage + 10) > queueSize ? queueSize : (repage + 10);
                List<BaseRows> list = rabbitService.getMessageList(ClientId, count);
                list = list.subList(repage, list.size());
                object.put("queueSize", queueSize);
                object.put("queue", list);
                return object.toJSONString();
            }
        } else if (Const.QUEUE_TYPE_KAFKA.equals(type)) {
            kafkaService.getConsumerGroup();
            object.put("queueSize", 0);
            object.put("queue", new ArrayList<>());
            return object.toJSONString();

        } else {
            ClientId =  namespace + "-" + clientName;
            object.put("queueSize", redissonClient.getMap(ClientId).size());
            RMap<String, JSONObject> map = redissonClient.getMap(ClientId,
                    new TypedJsonJacksonCodec(String.class, JSONObject.class));
            Collection<JSONObject> values = map.values();
            JSONArray array1 = new JSONArray();
            array1.addAll(values);
            object.put("queue", array1);
            return object.toJSONString();
        }
    }

    @Override
    public boolean deleteFromQueue(String namespace, String uuid, String errClient) {
        RMap<String, DataErrorMsg> map = redissonClient.getMap(namespace + "-" + errClient,
                new TypedJsonJacksonCodec(String.class, DataErrorMsg.class));
        map.remove(uuid);
        return true;
    }

    @Override
    public Result deleteTopic(String namespace, String clientInfoKey) {
        List<ClientInfo> clientInfos = clientOps.listConsumerClientsByKey(namespace, clientInfoKey);

        // 从EventHandler的发送列表中删除
        clientOps.removeConsumerClient(clientInfos);

        // 刪除对应队列中的topic
        Set<String> clientInfoSet = new HashSet<>();
        Iterator<ClientInfo> iterator = clientInfos.iterator();
        while (iterator.hasNext()) {
            ClientInfo clientInfo = iterator.next();
            String identify = clientInfo.getQueueType() + clientInfo.getKey();
            if (!clientInfoSet.add(identify)) {
                iterator.remove();
            }
        }

        int deleteSum = clientInfos.size();

        int successCount = 0;
        for (ClientInfo clientInfo : clientInfos) {
            // todo destroy
//           if (dataPublisher.destroy(clientInfo)) {
//               successCount ++;
//           }
        }


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("deleteSum", deleteSum);
        jsonObject.put("successCount", successCount);
        Result result = new Result(Result.SUCCESS, jsonObject.toJSONString());

        return result;
    }
}

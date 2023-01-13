package com.github.shawven.calf.track.server.web;

import com.github.shawven.calf.track.register.domain.ClientInfo;

import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface ClientService {
    /**
     * 添加服务器端及本地client信息
     *
     * @param namespace
     * @param clientInfo
     */
    void addClient(ClientInfo clientInfo, Integer partitions, Integer replication);

    /**
     * 客户端订阅信息
     *
     * @return
     */
    List<ClientInfo> listClient(String namespace, String queryType);

    /**
     * 客户端订阅信息
     *
     * @return
     */
    Map<String, List<ClientInfo>> listClientMap(String namespace);

    /**
     * 列出所有应用的错误队列
     * @return
     */
    List<String> listErrorClient(String namespace);

    /**
     * 删除服务器端及本地client信息
     * @param clientInfo
     */
    void deleteClient( ClientInfo clientInfo);

    /**
     * 获取日志文件状态：日志读到哪个文件的第几行
     */
    String getLogStatus(String namespace);

    /**
     * 获取应用队列长度
     */
    String getqueuesize(String namespace, String clientName,String type,int page);

    /**
     * 删除队列中该条记录
     *
     * @param namespace
     * @param uuid      :uuid
     * @param errClient 对列名
     * @return
     */
    boolean deleteFromQueue(String namespace, String uuid, String errClient);


    Result deleteTopic(String namespace, String clientInfoKey);
}

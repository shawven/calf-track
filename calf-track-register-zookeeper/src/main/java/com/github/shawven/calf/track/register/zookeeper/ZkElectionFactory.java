package com.github.shawven.calf.track.register.zookeeper;

import com.github.shawven.calf.track.register.ElectionFactory;
import com.github.shawven.calf.track.register.election.Election;
import com.github.shawven.calf.track.register.election.ElectionListener;
import org.apache.curator.framework.CuratorFramework;

/**
 * @author xw
 * @date 2022/11/30
 */
public class ZkElectionFactory implements ElectionFactory {

    private static final String DEFAULT_PATH = "/leader-selector/";

    private final CuratorFramework client;

    public ZkElectionFactory(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public Election getElection(String path, String namespace, String uniqueId,
                                long ttl, ElectionListener listener) {

        path = path != null
                ? path.endsWith("/") ? path : path.concat("/")
                : DEFAULT_PATH;

        return new ZkElection(client, path + namespace, uniqueId, ttl, true, listener);
    }
}

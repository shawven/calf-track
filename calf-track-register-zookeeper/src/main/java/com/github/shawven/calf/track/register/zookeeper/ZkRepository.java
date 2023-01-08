package com.github.shawven.calf.track.register.zookeeper;

import com.github.shawven.calf.track.register.Emitter;
import com.github.shawven.calf.track.register.Repository;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZkRepository implements Repository {

    private final Logger logger = LoggerFactory.getLogger(ZkRepository.class);

    private final CuratorFramework client;


    public ZkRepository(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public List<String> listTree(String prefix) {
        try {
            List<String> result = new ArrayList<>();
            List<String> paths = client.getChildren().forPath(prefix);
            for (String childPath : paths) {
                byte[] bytes = client.getData().forPath(prefix + "/" + childPath);
                result.add(new String(bytes));
            }
            return result;
        } catch (Exception e) {
            if (e instanceof KeeperException) {
                if (((KeeperException)e).code() == KeeperException.Code.NONODE) {
                    return Collections.emptyList();
                }
            }
            logger.error(e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public String get(String key) {
        try {
            byte[] bytes = client.getData().forPath(key);
            return new String(bytes);
        } catch (Exception e) {
            if (e instanceof KeeperException) {
                if (((KeeperException)e).code() == KeeperException.Code.NONODE) {
                    return null;
                }
            }
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void set(String key, String val) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if (stat != null) {
                update(key, val, stat);
            } else {
                create(key, val);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void set(String key, String val, long ttl) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if (stat != null) {
                update(key, val, stat);
            } else {
                create(key, val, ttl);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void del(String key) {
        try {
            client.delete().guaranteed().forPath(key);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void delTree(String prefix) {
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(prefix);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void create(String key, String val) throws Exception {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .forPath(key, val.getBytes(StandardCharsets.UTF_8));
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                logger.warn("node exist dispatch update");
                Stat stat = client.checkExists().forPath(key);
                update(key, val, stat);
                return;
            }
            logger.error("create error:{}", e.code());
        }
    }

    private void create(String key, String val, long ttl) throws Exception {
        try {
            client.create()
                    .withTtl(ttl * 1000).creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_WITH_TTL)
                    .forPath(key, val.getBytes(StandardCharsets.UTF_8));
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                logger.warn("node exist dispatch update");
                Stat stat = client.checkExists().forPath(key);
                update(key, val, stat);
                return;
            }
            logger.error("create error:{}", e.code());
        }
    }

    private void update(String key, String val, Stat stat) throws Exception {
        client.setData()
                .withVersion(stat.getVersion())
                .forPath(key, val.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void watch(String key, Emitter<String> emitter) {
        try {

            CuratorCache curatorCache = CuratorCache.build(client, key);

            curatorCache.listenable().addListener((type, oldData, data) -> {
                if (type == CuratorCacheListener.Type.NODE_CREATED
                || type == CuratorCacheListener.Type.NODE_CHANGED) {
                    emitter.onNext(new String(data.getData()));
                }
            });

            curatorCache.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

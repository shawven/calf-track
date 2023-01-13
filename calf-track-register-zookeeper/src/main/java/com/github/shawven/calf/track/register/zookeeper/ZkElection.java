package com.github.shawven.calf.track.register.zookeeper;

import com.github.shawven.calf.track.register.election.Election;
import com.github.shawven.calf.track.register.election.ElectionListener;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class ZkElection implements Election {

    private static final Logger logger = LoggerFactory.getLogger(Election.class);

    private final String name;
    /**
     * keep racing for leadership when losing
     */
    private final boolean requeue;

    private final ElectionListener listener;

    private final LeaderLatch leaderLatch;

    private final Stopwatch electWatch = Stopwatch.createUnstarted();

    private final AtomicBoolean running = new AtomicBoolean();

    public ZkElection(CuratorFramework client, String path, String name,
                      Long ttl, boolean requeue, ElectionListener listener) {
        this.name = name;
        this.requeue = requeue;
        this.listener = listener;
        this.leaderLatch = new LeaderLatch(client, path, this.name);
    }

    @Override
    public void start() {
        try {
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    running.set(true);
                    electWatch.stop();
                    logger.info("{} isLeader: cost:{}", name, electWatch);
                    electWatch.reset();

                    try {
                        listener.isLeader();
                        logger.info(name + " has successfully exec isLeader");
                    } catch (Exception e) {
                        logger.error(name + " exec isLeader error: " + e.getMessage(), e);
                    }
                }

                @Override
                public void notLeader() {
                    if (running.compareAndSet(true, false)) {
                        try {
                            listener.notLeader();
                            logger.info(name + " has successfully exec notLeader");
                        } catch (Exception e) {
                            logger.error(name + " exec notLeader error: " + e.getMessage(), e);
                        }
                    }

                    try {
                        Participant leader = leaderLatch.getLeader();
                        logger.info(name + " get current leader:{}", leader);
                    } catch (Exception e) {
                        logger.info(name + " get current leader error: " + e.getMessage(), e);
                    }
                }
            });
            leaderLatch.start();
        } catch (Exception e) {
            logger.info(String.format("%s elect error:%s", name, e.getMessage()));
            requeue();
        }
    }

    private void requeue() {
        if (running.compareAndSet(true, false)) {

            try {
                leaderLatch.close();
            } catch (IOException e) {
                logger.info(name + " closed error" + e.getMessage(), e);
            }

            try {
                listener.notLeader();
                logger.debug(name + " has successfully exec notLeader");
            } catch (Exception e) {
                logger.error(name + " exec notLeader error: " + e.getMessage(), e);
            }
        }

        if (requeue) {
            logger.info("{} prepare enqueue after 3 seconds", name);
            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            start();
        }
    }

    @Override
    public void close() {
        logger.info("{} closing", name);

        if (running.compareAndSet(true, false)) {
            try {
                leaderLatch.close();
            } catch (IOException e) {
                logger.info(name + " closed error" + e.getMessage(), e);
            }

            try {
                listener.notLeader();
                logger.info(name + " has successfully exec notLeader");
            } catch (Exception e) {
                logger.error(name + " exec notLeader error: " + e.getMessage(), e);
            }
        }

        logger.info("{} closed", name);
    }
}

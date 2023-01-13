package com.github.shawven.calf.track.register.etcd;

import com.github.shawven.calf.track.register.election.Election;
import com.github.shawven.calf.track.register.election.ElectionListener;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

class EtcdElection implements Election {

    private static final Logger logger = LoggerFactory.getLogger(Election.class);

    private final Client client;

    private final String path;

    private final String name;

    private final long ttl;

    private final boolean requeue;

    private final ElectionListener listener;

    private final Stopwatch electWatch = Stopwatch.createUnstarted();

    private final AtomicBoolean running = new AtomicBoolean();

    private CloseableClient closeableClient;

    private LeaderKey leaderKey;

    public EtcdElection(Client client, String path, String name,
                        Long ttl, boolean requeue, ElectionListener listener) {
        this.client = client;
        this.path = path;
        this.name = name != null ? name : UUID.randomUUID().toString();
        this.ttl = ttl;
        this.requeue = requeue;
        this.listener = listener;

    }

    @Override
    public void start() {
        String name = this.name;
        ByteSequence elect = ByteSequence.from(path + "/" + name, UTF_8);
        ByteSequence proposal = ByteSequence.from(name, UTF_8);

        logger.info("{} start elect", name);
        electWatch.start();

        try {
            long id = grantLeaseId();
            logger.info("{} grant leaseId {}", name, id);

            observer(name, elect, proposal);

            CampaignResponse campaignResponse = client.getElectionClient().campaign(elect, id, proposal).get();

            leaderKey = campaignResponse.getLeader();

            electWatch.stop();
            logger.info("{} campaignResponse elect:{} LeaderKey:{}, cost:{}", name, leaderKey.getName(), leaderKey.getKey(), electWatch);
            electWatch.reset();

            running.set(true);

            keepAlive(name, id);

            try {
                listener.isLeader();
                logger.info(name + " has successfully exec isLeader");
            } catch (Exception e) {
                logger.error(name + " exec isLeader error: " + e.getMessage(), e);
                throw e;
            }

        } catch (Exception e) {
            logger.error(name + " elect error: " + e.getMessage(), e);
            requeue();
        }
    }

    private void requeue() {
        logger.info("{} running: {}", name, running.get());
        if (running.compareAndSet(true, false)) {
            try {
                closeableClient.close();
                client.getElectionClient().resign(leaderKey);
            } catch (Exception e) {
                logger.error(name + " prepare requeue error: " + e.getMessage(), e);
            }

            try {
                listener.notLeader();
                logger.info(name + " has successfully exec notLeader");
            } catch (Exception e) {
                logger.error(name + " exec notLeader error: " + e.getMessage(), e);
            }

            if (requeue) {
                logger.info("{} prepare enqueue after 3 seconds", name);
                Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
                start();
            }
        }
    }

    private void observer(String name, ByteSequence elect, ByteSequence proposal) {
        client.getElectionClient().observe(elect, new io.etcd.jetcd.Election.Listener() {
            @Override
            public void onNext(LeaderResponse response) {
                KeyValue kv = response.getKv();
                if (!kv.getValue().equals(proposal)) {
                    logger.info("{} await for elect，current LeaderKey:{}, proposal:{}", name,
                            kv.getKey().toString(UTF_8), kv.getValue().toString(UTF_8));
                }
            }

            @Override
            public void onError(Throwable e) {
                logger.info("elect observe.onError:" + e.getMessage(), e);
                requeue();
            }

            @Override
            public void onCompleted() {
                logger.info("elect observe.onCompleted");
                requeue();
            }
        });
    }

    private long grantLeaseId() throws InterruptedException, ExecutionException {
        return client.getLeaseClient().grant(ttl, ttl, TimeUnit.SECONDS).get().getID();
    }

    private void keepAlive(String name, long id) {
        closeableClient = client.getLeaseClient().keepAlive(id, new StreamObserver<LeaseKeepAliveResponse>() {
            @Override
            public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {
                logger.debug("{} lease keepAlive.onNext ts:{}, ", name, leaseKeepAliveResponse.getTTL());
            }

            @Override
            public void onError(Throwable throwable) {
                logger.info("{} lease keepAlive.onError: {}", name, throwable.getMessage());
                requeue();
            }

            @Override
            public void onCompleted() {
                logger.info("{} lease keepAlive.onCompleted", name);
                requeue();
            }
        });
    }

    @Override
    public void close() {
        logger.info("{} closing, running: {}", name, running.get());

        if (running.compareAndSet(true, false)) {
            try {
                closeableClient.close();
                client.getElectionClient().resign(leaderKey);
            } catch (Exception e) {
                logger.error(name + " closed error: " + e.getMessage(), e);
            }

            try {
                listener.notLeader();
                logger.info("{} has successfully exec notLeader", name);
            } catch (Exception e) {
                logger.error(name + " exec notLeader error: " + e.getMessage(), e);
            }
        }

        logger.info("{} closed", name);
    }
}

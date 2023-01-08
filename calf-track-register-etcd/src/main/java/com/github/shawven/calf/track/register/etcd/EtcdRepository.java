package com.github.shawven.calf.track.register.etcd;

import com.github.shawven.calf.track.register.Emitter;
import com.github.shawven.calf.track.register.Repository;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class EtcdRepository implements Repository {

    private final Logger logger = LoggerFactory.getLogger(EtcdRepository.class);

    private final Client client;


    public EtcdRepository(Client client) {
        this.client = client;
    }

    @Override
    public List<String> listTree(String prefix) {
        GetResponse response;
        try {
            response = client.getKVClient().get(
                    ByteSequence.from(prefix, StandardCharsets.UTF_8),
                    GetOption.newBuilder().isPrefix(true).build()
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            return Collections.emptyList();
        }
        List<KeyValue> kvs = response.getKvs();
        if (kvs.isEmpty()) {
            return Collections.emptyList();
        }
        return response.getKvs().stream()
                .map(kv -> kv.getValue().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    @Override
    public String get(String key) {
        GetResponse response;
        try {
            response = client.getKVClient().get(ByteSequence.from(key, StandardCharsets.UTF_8)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
        List<KeyValue> kvs = response.getKvs();
        if (kvs.isEmpty()) {
            return null;
        }
        return kvs.get(0).getValue().toString(StandardCharsets.UTF_8);
    }

    @Override
    public void set(String key, String val) {
        client.getKVClient().put(
                ByteSequence.from(key, StandardCharsets.UTF_8),
                ByteSequence.from(val, StandardCharsets.UTF_8));
    }

    @Override
    public void set(String key, String val, long ttl) {
        client.getLeaseClient().grant(ttl).thenAccept(response -> {
            long leaseId = response.getID();
            client.getKVClient().put(
                    ByteSequence.from(key, StandardCharsets.UTF_8),
                    ByteSequence.from(val, StandardCharsets.UTF_8),
                    PutOption.newBuilder().withLeaseId(leaseId).build()
            );
        });
    }

    @Override
    public void del(String key) {
        client.getKVClient().delete(ByteSequence.from(key, StandardCharsets.UTF_8));
    }

    @Override
    public void delTree(String prefix) {
        client.getKVClient().delete(
                ByteSequence.from(prefix, StandardCharsets.UTF_8),
                DeleteOption.newBuilder().isPrefix(true).build()
        );
    }

    @Override
    public void watch(String key, Emitter<String> emitter) {
        client.getWatchClient().watch(
                ByteSequence.from(key, StandardCharsets.UTF_8),
                WatchOption.newBuilder().isPrefix(true).build(),
                new Watch.Listener() {
                    @Override
                    public void onNext(WatchResponse response) {
                        List<WatchEvent> events = response.getEvents();
                        for (WatchEvent event : events) {
                            if (WatchEvent.EventType.PUT.equals(event.getEventType())) {
                                String string = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                emitter.onNext(string);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        emitter.onError(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        emitter.onComplete();
                    }
                }
        );
    }
}

package com.github.shawven.calf.track.register;


import java.util.List;

public interface Repository {

    List<String> listChildren(String key);

    String get(String key);

    void set(String key, String val);

    void set(String key, String val, long ttl);

    void watch(String key, Emitter<String> emitter);
}

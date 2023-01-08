package com.github.shawven.calf.track.register;


import java.util.List;

public interface Repository {

    List<String> listTree(String prefix);

    String get(String key);

    void set(String key, String val);

    void set(String key, String val, long ttl);

    void del(String key);

    void delTree(String prefix);

    void watch(String key, Emitter<String> emitter);
}

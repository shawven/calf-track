package com.github.shawven.calf.track.client;


import com.github.shawven.calf.track.common.EventAction;
/**
 * @author xw
 * @date 2023-01-05
 */
public interface DataSubscribeHandler {

    String key();

    EventAction[] actions();

    void handle(String data);
}

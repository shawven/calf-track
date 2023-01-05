package com.github.shawven.calf.track.datasource.api;


import com.github.shawven.calf.track.datasource.api.domain.Command;

public interface ServerWatcher {

    void start(Command command);

    void stop(Command command);
}

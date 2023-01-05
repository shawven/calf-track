package com.github.shawven.calf.track.register.election;


import java.io.Closeable;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface Election extends Closeable {

    /**
     * start racing for leadership.
     */
    void start();


    @Override
    void close();
}

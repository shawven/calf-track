package com.github.shawven.calf.track.server;

import com.github.shawven.calf.track.datasource.api.TrackServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
public class TrackServerRunner implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(TrackServerRunner.class);

    private final List<TrackServer> serverList;

    private volatile boolean running;

    public TrackServerRunner(List<TrackServer> serverList) {
        this.serverList = serverList;
    }

    @Override
    public void start() {
        logger.info("starting trackServer");
        for (TrackServer server : serverList) {
            server.start();
            logger.info("trackServer {} started", server.dataSourceType());
        }
        running = true;
    }

    @Override
    public void stop() {
        logger.info("stopping trackServer");
        for (TrackServer server : serverList) {
            try {
                server.stop();
                logger.info("trackServer {} stopped", server.dataSourceType());
            } catch (Exception e) {
                logger.info("trackServer {} stop error: " + e.getMessage(), e);
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}

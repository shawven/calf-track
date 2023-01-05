package com.github.shawven.calf.track.server;

import com.github.shawven.calf.track.datasource.api.TrackServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.util.List;

/**
 * @author xw
 * @date 2023-01-05
 */
public class TrackServerRunner implements ApplicationRunner {

    private final Logger logger = LoggerFactory.getLogger(TrackServerRunner.class);

    private final List<TrackServer> serverList;

    public TrackServerRunner(List<TrackServer> serverList) {
        this.serverList = serverList;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) {
        logger.info("trackServer start");
        for (TrackServer server : serverList) {
            server.start();
            logger.info("trackServer {} started", server.dataSourceType());
        }
        logger.info("trackServer started");
    }
}

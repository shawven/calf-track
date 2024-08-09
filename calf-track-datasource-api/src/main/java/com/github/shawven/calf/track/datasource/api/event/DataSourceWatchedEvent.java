package com.github.shawven.calf.track.datasource.api.event;

import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author xw
 * @date 2024/8/9
 */
@Data
@AllArgsConstructor
public class DataSourceWatchedEvent {

    DataSourceCfg dataSourceCfg;

    String database;

    String table;
}

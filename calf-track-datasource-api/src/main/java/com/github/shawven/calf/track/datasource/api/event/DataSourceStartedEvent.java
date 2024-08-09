package com.github.shawven.calf.track.datasource.api.event;

import com.github.shawven.calf.track.register.domain.DataSourceCfg;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author xw
 * @date 2024/8/8
 */
@Data
@AllArgsConstructor
public class DataSourceStartedEvent {

    DataSourceCfg dataSourceCfg;
}

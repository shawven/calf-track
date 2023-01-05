package com.github.shawven.calf.track.datasource.api.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * @author xw
 * @date 2023-01-05
 */
@Data
public class DataErrorMsg implements Serializable {

    private static final long serialVersionUID = 4556335110286780329L;

    private Exception exception;
    private String dataKey;

}

package com.github.shawven.calf.track.datasource.api.domain;


import com.github.shawven.calf.track.common.EventAction;

import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public class InsertRows extends BaseRows {

    private static final long serialVersionUID = 6443935897277661139L;

    private List<Map<String, Object>> rowMaps;

    public InsertRows() {
    }

    public InsertRows(BaseRows baseRows, List<Map<String, Object>> rowMaps) {
        super(baseRows.getDsName(), baseRows.getEventAction(), baseRows.getDatabase(), baseRows.getTable());
        super.setEventAction(EventAction.INSERT);
        this.rowMaps = rowMaps;
    }

    public List<Map<String, Object>> getRowMaps() {
        return rowMaps;
    }

    public void setRowMaps(List<Map<String, Object>> rowMaps) {
        this.rowMaps = rowMaps;
    }

    @Override
    public String toString() {
        return "WriteRowsDTO{" +
                "rowMaps=" + rowMaps +
                "} " + super.toString();
    }
}

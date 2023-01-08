package com.github.shawven.calf.track.datasource.api.domain;


import com.github.shawven.calf.track.common.EventAction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public class DeleteRows extends BaseRows implements Serializable {

    private static final long serialVersionUID = -958814764356190600L;

    private List<Map<String, Object>> rowMaps;

    public DeleteRows() {
    }

    public DeleteRows(BaseRows baseRows, List<Map<String, Object>> rowMaps) {
        super(baseRows.getDsName(), baseRows.getEventAction(), baseRows.getDatabase(), baseRows.getTable());
        super.setEventAction(EventAction.DELETE);
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
        return "DeleteRowsDTO{" +
                "rowMaps=" + rowMaps +
                "} " + super.toString();
    }
}

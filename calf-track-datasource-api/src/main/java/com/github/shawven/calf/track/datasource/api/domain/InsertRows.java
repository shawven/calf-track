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


    public InsertRows(String namespace, String dsName, String destQueue, String database, String table) {
        super(namespace, dsName, destQueue, EventAction.INSERT, database, table);
    }

    public static InsertRows convertForm(BaseRows baseRows) {
        return new InsertRows(baseRows.getNamespace(), baseRows.getDsName(),
                baseRows.getDestQueue(), baseRows.getDatabase(), baseRows.getTable());
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

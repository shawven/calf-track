package com.github.shawven.calf.track.datasource.api.domain;


import com.github.shawven.calf.track.common.EventAction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public class UpdateRows extends BaseRows implements Serializable {

    private static final long serialVersionUID = 3615869271596131001L;

    private List<Row> rows;

    public UpdateRows() {
    }

    public UpdateRows(BaseRows baseRows, List<Row> rows) {
        super(baseRows.getNamespace(), baseRows.getEventAction(), baseRows.getDatabase(), baseRows.getTable());
        super.setEventAction(EventAction.UPDATE);
        this.rows = rows;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "UpdateRowsDTO{" +
                "rows=" + rows +
                "} " + super.toString();
    }

    public static class Row implements Serializable {

        private static final long serialVersionUID = -2966621316372838979L;

        private Map<String, Object> beforeRowMap;
        private Map<String, Object> afterRowMap;

        public Row() {

        }

        public Row(Map<String, Object> beforeRowMap, Map<String, Object> afterRowMap) {
            this.beforeRowMap = beforeRowMap;
            this.afterRowMap = afterRowMap;
        }

        public Map<String, Object> getBeforeRowMap() {
            return beforeRowMap;
        }

        public void setBeforeRowMap(Map<String, Object> beforeRowMap) {
            this.beforeRowMap = beforeRowMap;
        }

        public Map<String, Object> getAfterRowMap() {
            return afterRowMap;
        }

        public void setAfterRowMap(Map<String, Object> afterRowMap) {
            this.afterRowMap = afterRowMap;
        }

        @Override
        public String toString() {
            return "UpdateRow{" +
                    "beforeRowMap=" + beforeRowMap +
                    ", afterRowMap=" + afterRowMap +
                    '}';
        }
    }
}

package com.github.shawven.calf.track.datasource.mongo;

import com.github.shawven.calf.track.common.EventAction;
import com.github.shawven.calf.track.datasource.api.domain.BaseRows;
import com.github.shawven.calf.track.datasource.api.domain.DeleteRows;
import com.github.shawven.calf.track.datasource.api.domain.UpdateRows;
import com.github.shawven.calf.track.datasource.api.domain.InsertRows;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xw
 * @date 2023-01-05
 */
public interface OpLogEventFormatter {
    /**
     * 格式化参数格式
     *
     * @param event
     * @return 格式化后的string
     */
    BaseRows format(Document event);

    class BaseFormatter implements OpLogEventFormatter {

        private final String namespace;
        private final String dsName;
        private final String destQueue;

        public BaseFormatter(String namespace, String dsName, String destQueue) {
            this.namespace = namespace;
            this.dsName = dsName;
            this.destQueue = destQueue;
        }

        @Override
        public BaseRows format(Document event) {
            return new BaseRows(namespace, dsName, destQueue, null,
                    DocumentUtils.getDataBase(event), DocumentUtils.getTable(event));
        }
    }


    class Write extends BaseFormatter {

        public Write(String namespace, String dsName, String destQueue) {
            super(namespace, dsName, destQueue);
        }

        @Override
        public BaseRows format(Document event) {
            InsertRows insertRows = InsertRows.convertForm(super.format(event));

            //添加列映射
            Document context = (Document) event.get(OpLogClientFactory.CONTEXT_KEY);
            List<Map<String, Object>> urs = new ArrayList<>();
            urs.add(context);
            insertRows.setRowMaps(urs);
            return insertRows;
        }
    }

    class Update extends BaseFormatter {

        public Update(String namespace, String dsName, String destQueue) {
            super(namespace, dsName, destQueue);
        }

        @Override
        public BaseRows format(Document event) {
            UpdateRows updateRows = UpdateRows.convertForm(super.format(event));

            //添加列映射
            List<UpdateRows.Row> urs = new ArrayList<>();
            Document updateWhere = (Document)event.get(OpLogClientFactory.UPDATE_WHERE_KEY);
            Document context = (Document) event.get(OpLogClientFactory.CONTEXT_KEY);
            context = (Document) context.get(OpLogClientFactory.UPDATE_CONTEXT_KEY);
            urs.add(new UpdateRows.Row(updateWhere,context));
            updateRows.setRows(urs);
            return updateRows;
        }
    }

    class Delete extends BaseFormatter {

        public Delete(String namespace, String dsName, String destQueue) {
            super(namespace, dsName, destQueue);
        }

        @Override
        public BaseRows format(Document event) {
            DeleteRows deleteRows =DeleteRows.convertForm(super.format(event));

            //添加列映射
            Document context = (Document) event.get(OpLogClientFactory.CONTEXT_KEY);
            List<Map<String, Object>> urs = new ArrayList<>();
            urs.add(context);
            deleteRows.setRowMaps(urs);
            return deleteRows;
        }
    }
}

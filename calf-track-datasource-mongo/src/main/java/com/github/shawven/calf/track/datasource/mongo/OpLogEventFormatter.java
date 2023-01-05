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


    class Write implements OpLogEventFormatter {

        private final String namespace;

        public Write(String namespace) {
            this.namespace = namespace;
        }

        @Override
        public BaseRows format(Document event) {
            InsertRows insertRows = new InsertRows();
            insertRows.setEventAction(EventAction.INSERT);
            //添加表信息
            insertRows.setDatabase(DocumentUtils.getDataBase(event));
            insertRows.setTable(DocumentUtils.getTable(event));
            insertRows.setNamespace(namespace);
            //添加列映射
            Document context = (Document) event.get(OpLogClientFactory.CONTEXT_KEY);
            List<Map<String, Object>> urs = new ArrayList<>();
            urs.add(context);
            insertRows.setRowMaps(urs);
            return insertRows;
        }
    }

    class Update implements OpLogEventFormatter {

        private final String namespace;

        public Update(String namespace) {
            this.namespace = namespace;
        }

        @Override
        public BaseRows format(Document event) {
            UpdateRows updateRows = new UpdateRows();
            updateRows.setEventAction(EventAction.UPDATE);
            //添加表信息
            updateRows.setDatabase(DocumentUtils.getDataBase(event));
            updateRows.setTable(DocumentUtils.getTable(event));
            updateRows.setNamespace(namespace);
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

    class Delete implements OpLogEventFormatter {

        private final String namespace;

        public Delete(String namespace) {
            this.namespace = namespace;
        }

        @Override
        public BaseRows format(Document event) {
            DeleteRows deleteRows = new DeleteRows();
            deleteRows.setEventAction(EventAction.DELETE);
            //添加表信息
            deleteRows.setDatabase(DocumentUtils.getDataBase(event));
            deleteRows.setTable(DocumentUtils.getTable(event));
            deleteRows.setNamespace(namespace);
            //添加列映射
            Document context = (Document) event.get(OpLogClientFactory.CONTEXT_KEY);
            List<Map<String, Object>> urs = new ArrayList<>();
            urs.add(context);
            deleteRows.setRowMaps(urs);
            return deleteRows;
        }
    }
}

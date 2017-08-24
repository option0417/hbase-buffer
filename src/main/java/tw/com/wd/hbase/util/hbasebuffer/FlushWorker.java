package tw.com.wd.hbase.util.hbasebuffer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.wd.hbase.util.hbasebuffer.obj.HBaseBufferItem;

import java.util.*;
import java.util.concurrent.Callable;

public class FlushWorker implements Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(FlushWorker.class.getSimpleName());
    private Connection hConn;
    private List<HBaseBufferItem> bufferItemList;

    public FlushWorker(Connection hConn, List<HBaseBufferItem> bufferItemList) {
        this.hConn          = hConn;
        this.bufferItemList = bufferItemList;
    }

    public Void call() throws Exception {
        Map<TableName, List<Row>> tblRowMap = new HashMap<TableName, List<Row>>();

        for (HBaseBufferItem hBaseBufferItem : bufferItemList) {
            if (tblRowMap.get(hBaseBufferItem.getTblName()) == null) {
                tblRowMap.put(hBaseBufferItem.getTblName(), new ArrayList<Row>());
            }
            List<Row> rowList = tblRowMap.get(hBaseBufferItem.getTblName());
            rowList.add(hBaseBufferItem.getHbaseOp());
        }

        Iterator<Map.Entry<TableName, List<Row>>> iter = tblRowMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<TableName, List<Row>> entry = iter.next();

            Table tbl = null;
            try {
                tbl = this.hConn.getTable(entry.getKey());
                tbl.batch(entry.getValue(), new Object[entry.getValue().size()]);

                LOG.info("{} rows flushed to {}", entry.getValue().size(), entry.getKey().getNameAsString());
            } finally {
                if (tbl != null) {
                    tbl.close();
                }
            }

        }

        return null;
    }
}

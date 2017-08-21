package tw.com.wd.hbase.util.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.wd.hbase.exception.HBufferException;
import tw.com.wd.hbase.util.HBaseBufferConf;
import tw.com.wd.hbase.util.IHBaseBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HBaseBuffer implements IHBaseBuffer {
    private static Logger LOG = LoggerFactory.getLogger(HBaseBuffer.class.getName());
    private static final ConcurrentMap<TableName, BlockingQueue<Row>> HBASE_OP_MAP = new ConcurrentHashMap<TableName, BlockingQueue<Row>>();
    private int bufferSize;
    private long offerTimeout;
    private Connection hConn;


    public HBaseBuffer(Connection hConn) {
        super();
        this.hConn          = hConn;
        this.bufferSize     = HBaseBufferConf.getBufferSize();
        this.offerTimeout   = HBaseBufferConf.getOfferTimeout();
    }

    public boolean put(Row hbaseOp, TableName tbl) {
        try {
            if (this.HBASE_OP_MAP.get(tbl) == null) {
                synchronized (this.HBASE_OP_MAP) {
                    if (this.HBASE_OP_MAP.get(tbl) == null) {
                        this.HBASE_OP_MAP.put(tbl, new ArrayBlockingQueue<Row>(bufferSize));
                    }
                }
            }

            while (!this.HBASE_OP_MAP.get(tbl).offer(hbaseOp, offerTimeout, TimeUnit.MILLISECONDS)) {
                this.flush(false);
            }
            return true;
        } catch (Exception e) {
            showException(e);
            return false;
        }
    }

    public void flush() {
        flush(true);
    }

    protected void flush(boolean force) {
        Iterator<Map.Entry<TableName, BlockingQueue<Row>>> iter = HBASE_OP_MAP.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<TableName, BlockingQueue<Row>> entry = iter.next();

            if (!force) {
                if (entry.getValue().size() < bufferSize) {
                    synchronized (entry.getValue()) {
                        if (entry.getValue().size() < bufferSize) {
                            return;
                        }
                    }
                }
            }

            List<Row> rowList = new ArrayList<Row>(entry.getValue().size());
            entry.getValue().drainTo(rowList);

            try {
                flushToHBase(entry.getKey(), rowList);
            } catch (Exception e) {
                showException(e);
                throw new HBufferException(e);
            }
        }
    }

    private void flushToHBase(TableName tblName, List<Row> rowList) throws IOException, InterruptedException {
        if (rowList.size() == 0) {
            return;
        }

        Table tbl = null;
        try {
            tbl = this.hConn.getTable(tblName);
            tbl.batch(rowList, new Object[rowList.size()]);
        } finally {
            if (tbl != null) {
                tbl.close();
            }
        }
        LOG.info("{} rows flushed to {}", rowList.size(), tblName.getNameAsString());
    }

    private void showException(Exception e) {
        LOG.error("Catch error: {}\n", e.getMessage());
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        for (int idx = 0; idx < stackTraceElements.length || idx < 5; idx++) {
            LOG.error(stackTraceElements[idx].toString());
        }
    }
}

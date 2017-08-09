package tw.com.wd.hbase.util.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.wd.hbase.exception.HBufferException;
import tw.com.wd.hbase.util.IHBaseBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HBaseBuffer implements IHBaseBuffer {
    private Logger LOG = LoggerFactory.getLogger(HBaseBuffer.class.getName());
    private int DEFAULT_BUFFER_SIZE = 10000;
    private long OFFER_TIMEOUT      = 300l;
    private ConcurrentMap<TableName, BlockingQueue<Row>> hbaseOPMap;
    private Connection hConn;


    public HBaseBuffer(Connection hConn) {
        super();
        this.hConn          = hConn;
        this.hbaseOPMap     = new ConcurrentHashMap<TableName, BlockingQueue<Row>>();
    }

    public boolean put(Row hbaseOp, TableName tbl) {
        try {
            if (this.hbaseOPMap.get(tbl) == null) {
                synchronized (this) {
                    if (this.hbaseOPMap.get(tbl) == null) {
                        this.hbaseOPMap.put(tbl, new ArrayBlockingQueue<Row>(DEFAULT_BUFFER_SIZE));
                    }
                }
            }

            while (!this.hbaseOPMap.get(tbl).offer(hbaseOp, OFFER_TIMEOUT, TimeUnit.MILLISECONDS)) {
                this.flush();
            }
            return true;
        } catch (Exception e) {
            showException(e);
            return false;
        }
    }

    public void flush() {
        Iterator<Map.Entry<TableName, BlockingQueue<Row>>> iter = hbaseOPMap.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<TableName, BlockingQueue<Row>> entry = iter.next();

            if (entry.getValue().size() == DEFAULT_BUFFER_SIZE) {
                synchronized (this) {
                    if (entry.getValue().size() == DEFAULT_BUFFER_SIZE) {
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
            }
        }
    }

    private void flushToHBase(TableName tal, List<Row> rowList) throws IOException, InterruptedException {
        Table tbl = this.hConn.getTable(tal);

        try {
            tbl.batch(rowList, new Object[rowList.size()]);
        } finally {
            tbl.close();
        }
    }

    private void showException(Exception e) {
        LOG.error("Catch error: {}\n", e.getMessage());
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        for (int idx = 0; idx < stackTraceElements.length || idx < 5; idx++) {
            LOG.error(stackTraceElements[idx].toString());
        }
    }
}

package tw.com.wd.hbase.util.hbasebuffer.impl;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.wd.hbase.util.hbasebuffer.BufferThreadPool;
import tw.com.wd.hbase.util.hbasebuffer.FlushWorker;
import tw.com.wd.hbase.util.hbasebuffer.HBaseBufferConf;
import tw.com.wd.hbase.util.hbasebuffer.IHBaseBuffer;
import tw.com.wd.hbase.util.hbasebuffer.exception.HBufferException;
import tw.com.wd.hbase.util.hbasebuffer.obj.HBaseBufferItem;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class HBaseBuffer implements IHBaseBuffer {
    private static final Logger LOG;
    private static final BlockingQueue<HBaseBufferItem> BUFFER_ITEM_QUEUE;
    private static final int BUFFER_SIZE;
    private static final long OFFER_TIMEOUT;
    private Connection hConn;

    static {
        LOG                 = LoggerFactory.getLogger(HBaseBuffer.class.getName());
        BUFFER_SIZE         = HBaseBufferConf.getBufferSize();
        OFFER_TIMEOUT       = HBaseBufferConf.getOfferTimeout();
        BUFFER_ITEM_QUEUE   = new ArrayBlockingQueue<HBaseBufferItem>(BUFFER_SIZE);
    }


    public HBaseBuffer(Connection hConn) {
        super();
        this.hConn = hConn;
    }

    public boolean put(Row hbaseOp, TableName tblName) {
        try {
            HBaseBufferItem hBaseBufferItem = buildBufferItem(hbaseOp, tblName);

            while (!BUFFER_ITEM_QUEUE.offer(hBaseBufferItem, OFFER_TIMEOUT, TimeUnit.MILLISECONDS)) {
                this.flush(false);
            }
            return true;
        } catch (Exception e) {
            showException(e);
            return false;
        }
    }

    private HBaseBufferItem buildBufferItem(Row hbaseOp, TableName tblName) {
        return new HBaseBufferItem(hbaseOp, tblName);
    }

    public void flush() {
        flush(true);
    }

    protected void flush(boolean force) {

        List<HBaseBufferItem> bufferItemList = new ArrayList<HBaseBufferItem>(BUFFER_ITEM_QUEUE.size());
        BUFFER_ITEM_QUEUE.drainTo(bufferItemList);

        try {
            flushToHBase(bufferItemList);
        } catch (Exception e) {
            showException(e);
            throw new HBufferException(e);
        }
    }

    private void flushToHBase(List<HBaseBufferItem> bufferItemList) {
        FlushWorker flushWorker = new FlushWorker(this.hConn, bufferItemList);

        BufferThreadPool.getInstance().submit(flushWorker);
    }

    private void showException(Exception e) {
        LOG.error("Catch error: {}\n", e.getMessage());
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        for (int idx = 0; idx < stackTraceElements.length || idx < 5; idx++) {
            LOG.error(stackTraceElements[idx].toString());
        }
    }
}

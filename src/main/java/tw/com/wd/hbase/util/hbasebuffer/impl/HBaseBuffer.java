package tw.com.wd.hbase.util.hbasebuffer.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.wd.hbase.util.hbasebuffer.*;
import tw.com.wd.hbase.util.hbasebuffer.exception.HBufferException;
import tw.com.wd.hbase.util.hbasebuffer.obj.HBaseBufferItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class HBaseBuffer implements IHBaseBuffer {
    private static final Logger LOG;
    private static final BlockingQueue<HBaseBufferItem> BUFFER_ITEM_QUEUE;
    private static final int BUFFER_SIZE;
    private static final long OFFER_TIMEOUT;
    private Connection hConn;
    private ExecutorService instanceExecutor;
    private boolean isActive;
    private boolean isExecutorActive;


    static {
        LOG                 = LoggerFactory.getLogger(HBaseBuffer.class.getName());
        BUFFER_SIZE         = HBaseBufferConf.getBufferSize();
        OFFER_TIMEOUT       = HBaseBufferConf.getOfferTimeout();
        BUFFER_ITEM_QUEUE   = new ArrayBlockingQueue<HBaseBufferItem>(BUFFER_SIZE);
    }

    public static HBaseBuffer getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private static final class InstanceHolder {
        private static final HBaseBuffer INSTANCE = new HBaseBuffer();
    }
    private HBaseBuffer() {
        super();
        initial();
        startHBaseBuffer();
        LOG.info("HBaseBuffer Started");
    }

    private void initial() {
        setupHBaseBuffer();
        BufferShutdownHook.getInstance().addShutdownable(this);
        instanceExecutor = Executors.newSingleThreadExecutor(new BufferThreadFactory());
    }

    private void startHBaseBuffer() {
        HBaseBufferExecutor hBaseBufferExecutor = new HBaseBufferExecutor(this);
        instanceExecutor.submit(hBaseBufferExecutor);
    }

    private void setupHBaseBuffer() {
        Configuration hconf         = createConfiguration();
        ExecutorService connPool    = createConnectionPool();

        Connection hConn = null;
        try {
            hConn = ConnectionFactory.createConnection(hconf, connPool);
        } catch (Exception e) {
            LOG.error("Caught Exception: ", e);
        }
        this.hConn = hConn;

        isActive = isExecutorActive = true;
    }

    private Configuration createConfiguration() {
        if (HBaseBufferConf.getZookeeperQuorum() == null || HBaseBufferConf.getZookeeperQuorum().length() == 0 ||
                HBaseBufferConf.getHBaseRootDir() == null || HBaseBufferConf.getHBaseRootDir().length() == 0) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM or HBASE_ROOT_DIR == null");
        }
        LOG.info("Set ZookeeperQuorum to {}", HBaseBufferConf.getZookeeperQuorum());
        LOG.info("Set HBaseRootDir to {}", HBaseBufferConf.getHBaseRootDir());

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", HBaseBufferConf.getZookeeperQuorum());
        hbaseConf.set("hbase.root.dir", HBaseBufferConf.getHBaseRootDir());
        return hbaseConf;
    }

    private ExecutorService createConnectionPool() {
        final int THREAD_CORE_SIZE = Runtime.getRuntime().availableProcessors() << 1;
        final int TASK_QUEUE_SIZE = (THREAD_CORE_SIZE << 3) + (THREAD_CORE_SIZE << 1);
        LOG.info("Set THREAD_CORE_SIZE to {}", THREAD_CORE_SIZE);
        LOG.info("Set TASK_QUEUE_SIZE to {}", TASK_QUEUE_SIZE);

        ExecutorService hConnPool =
                new ThreadPoolExecutor(
                        Runtime.getRuntime().availableProcessors() << 1,
                        THREAD_CORE_SIZE + (THREAD_CORE_SIZE >> 1),
                        100l,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(TASK_QUEUE_SIZE));
        return hConnPool;
    }

    private void restart() {
        if (isActive) {
            instanceExecutor.shutdownNow();
            instanceExecutor = Executors.newSingleThreadExecutor(new BufferThreadFactory());
            startHBaseBuffer();
        }
    }

    public boolean put(Row hbaseOp, TableName tblName) {
        try {
            HBaseBufferItem hBaseBufferItem = buildBufferItem(hbaseOp, tblName);

            while (!BUFFER_ITEM_QUEUE.offer(hBaseBufferItem, OFFER_TIMEOUT, TimeUnit.MILLISECONDS)) {
                this.flush();
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
        List<HBaseBufferItem> bufferItemList = new ArrayList<HBaseBufferItem>(BUFFER_ITEM_QUEUE.size());
        BUFFER_ITEM_QUEUE.drainTo(bufferItemList);

        try {
            flushToHBase(bufferItemList);
        } catch (Exception e) {
            showException(e);
            throw new HBufferException(e);
        }
    }

    public int size() {
        return BUFFER_ITEM_QUEUE.size();
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

    public void shutdown() {
        this.isActive = false;

        instanceExecutor.shutdown();

        // Close HConnection when JVM shutdown
        // ExecutorService will shutdown by HConnection
        if (this.hConn != null && !this.hConn.isClosed()) {
            try {
                this.hConn.close();
            } catch (IOException e) {
                // skip
            }
        }
    }

    private class BufferThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r, "HBUF");
        }
    }

    private class HBaseBufferExecutor implements Runnable {
        private HBaseBuffer instance;

        private HBaseBufferExecutor(HBaseBuffer instance) {
            this.instance   = instance;
        }

        public void run() {
            try {
                while (isActive && isExecutorActive) {
                    instance.flush();
                    Thread.sleep(OFFER_TIMEOUT);
                }
            } catch (Throwable t) {
                LOG.error("Catch error: {}\n", t.getMessage());
                LOG.error("Prepare restart HBaseBuffer");
                isExecutorActive = false;
            } finally {
                instance.restart();
            }
        }
    }
}

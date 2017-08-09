package tw.com.wd.hbase.util.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.junit.*;
import tw.com.wd.hbase.util.IHBaseBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 * 1000 Worker do 100 = 100000 put and done in 1998 millitime
 * 1000 Worker do 500 = 500000 put and done in 5393 millitime
 * 1000 Worker do 1000 = 1000000 put and done in 9320 millitime
 * 1500 Worker do 1000 = 1500000 put and done in 12467 millitime
 */
public class HBaseBufferTest {
    private static final int THREAD_CORE_SIZE                   = Runtime.getRuntime().availableProcessors() << 1;
    private static final int TASK_QUEUE_SIZE                    = (THREAD_CORE_SIZE << 3) + (THREAD_CORE_SIZE << 1);
    private static ExecutorService hConnPool                    = null;
    private static final String HBASE_ENV_KEY_ZOOKEEPER_QUORUM  = "hbase.zookeeper.quorum";
    private static final String HBASE_ENV_ROOT_DIR              = "hbase.root.dir";
    private static final int WORKER_SIZE                        = 1000;
    private static final int PUT_COUNT                          = 100;
    private static final TableName TBL1                         = TableName.valueOf("testBuffer1");
    private static final TableName TBL2                         = TableName.valueOf("testBuffer2");
    private static final TableName TBL3                         = TableName.valueOf("testBuffer3");

    private static Configuration conf;
    private static Connection hConn;

    private IHBaseBuffer hbaseBuffer;
    private ExecutorService workerPool;
    private List<Future<Boolean>> futureList;

    @BeforeClass
    public static void setupClass() throws IOException {
        hConnPool =
                new ThreadPoolExecutor(
                        THREAD_CORE_SIZE,
                        THREAD_CORE_SIZE + (THREAD_CORE_SIZE >> 1),
                        1l,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(TASK_QUEUE_SIZE));

        conf = new Configuration();
        conf.set(HBASE_ENV_KEY_ZOOKEEPER_QUORUM, "nqmi11");
        conf.set(HBASE_ENV_ROOT_DIR, "hdfs://nqmi11:8020/hbase");
        hConn = ConnectionFactory.createConnection(conf, hConnPool);

        Admin admin = hConn.getAdmin();

        if (!admin.tableExists(TBL1)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TBL1);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf");
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }

        if (!admin.tableExists(TBL2)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TBL2);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf");
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }

        if (!admin.tableExists(TBL3)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TBL3);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf");
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }
        admin.close();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        Admin admin = hConn.getAdmin();

        if (admin.tableExists(TBL1)) {
            admin.disableTable(TBL1);
            admin.deleteTable(TBL1);
        }

        if (admin.tableExists(TBL2)) {
            admin.disableTable(TBL2);
            admin.deleteTable(TBL2);
        }

        if (admin.tableExists(TBL3)) {
            admin.disableTable(TBL3);
            admin.deleteTable(TBL3);
        }

        admin.close();
        hConn.close();
    }

    @Before
    public void preTest() throws IOException {
        hbaseBuffer = new HBaseBuffer(hConn);
        workerPool = Executors.newFixedThreadPool(WORKER_SIZE);
        futureList = new ArrayList<Future<Boolean>>(WORKER_SIZE);
    }

    @After
    public void postTest() throws IOException {
        Admin admin = hConn.getAdmin();

        if (admin.tableExists(TBL1)) {
            admin.disableTable(TBL1);
            admin.truncateTable(TBL1, true);
        }

        if (admin.tableExists(TBL2)) {
            admin.disableTable(TBL2);
            admin.truncateTable(TBL2, true);
        }

        if (admin.tableExists(TBL3)) {
            admin.disableTable(TBL3);
            admin.truncateTable(TBL3, true);
        }
        admin.close();
    }

    @Test
    public void testPut() {
        long startime = System.currentTimeMillis();
        try {

            for (int cnt = 0; cnt < WORKER_SIZE; cnt++) {
                futureList.add(workerPool.submit(new PutWorker(cnt, TBL1, hbaseBuffer, PUT_COUNT)));
            }

            int  cnt = 0;
            while (cnt != futureList.size()) {
                cnt = 0;
                for (int idx = 0; idx < futureList.size(); idx++) {
                    if (futureList.get(idx).isDone()) {
                        cnt++;
                    }
                }
            }
            hbaseBuffer.flush();
            long endtime = System.currentTimeMillis();
            System.out.printf("%d Worker do %d put and done in %d millitime\n", WORKER_SIZE, PUT_COUNT, endtime - startime);

            for (Future<Boolean> f : futureList) {
                assertThat(f.get(), is(Boolean.TRUE));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    @Test
    public void testPutMultiTable() {
        long startime = System.currentTimeMillis();
        try {

            for (int cnt = 0; cnt < WORKER_SIZE; cnt++) {
                if (cnt % 3 == 0) {
                    futureList.add(workerPool.submit(new PutWorker(cnt, TBL1, hbaseBuffer, PUT_COUNT)));
                } else if (cnt % 3 == 1) {
                    futureList.add(workerPool.submit(new PutWorker(cnt, TBL2, hbaseBuffer, PUT_COUNT)));
                } else {
                    futureList.add(workerPool.submit(new PutWorker(cnt, TBL3, hbaseBuffer, PUT_COUNT)));
                }
            }

            int  cnt = 0;
            while (cnt != futureList.size()) {
                cnt = 0;
                for (int idx = 0; idx < futureList.size(); idx++) {
                    if (futureList.get(idx).isDone()) {
                        cnt++;
                    }
                }
            }
            hbaseBuffer.flush();
            long endtime = System.currentTimeMillis();
            System.out.printf("%d Worker do %d put and done in %d millitime\n", WORKER_SIZE, PUT_COUNT, endtime - startime);

            for (Future<Boolean> f : futureList) {
                assertThat(f.get(), is(Boolean.TRUE));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class PutWorker implements Callable<Boolean> {
        private int id;
        private int putCount;
        private TableName tbl;
        private IHBaseBuffer hbaseBuffer;

        public PutWorker(int id, TableName tbl, IHBaseBuffer hbaseBuffer, int putCount) {
            super();
            this.id             = id;
            this.tbl            = tbl;
            this.putCount       = putCount;
            this.hbaseBuffer    = hbaseBuffer;
        }

        public Boolean call() throws Exception {
            boolean flag = true;
            for (int cnt = 0; cnt < putCount; cnt++) {
                Put put = new Put((id + "_" + cnt).getBytes());
                put.addColumn("cf".getBytes(), "cq".getBytes(), (id + "_" + cnt).getBytes());
                if(!this.hbaseBuffer.put(put, tbl)) {
                    flag = false;
                }
            }
            return flag;
        }
    }
}

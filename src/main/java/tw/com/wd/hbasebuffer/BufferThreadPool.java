package tw.com.wd.hbasebuffer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferThreadPool {
    private ThreadPoolExecutor bufferPoolExecutor;


    private static final class InstanceHolder {
        private static final BufferThreadPool INSTANCE = new BufferThreadPool();
    }

    private  BufferThreadPool() {
        super();
        int core = Runtime.getRuntime().availableProcessors() << 1;
        bufferPoolExecutor = new ThreadPoolExecutor(
                core,
                core,
                HBaseBufferConf.getOfferTimeout(),
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>((core << 6) + (core << 5) + (core << 2)),
                new BufferThreadFactory(),
                new BufferRejectedExecutionHandler());
    }

    public static final BufferThreadPool getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public void submit(Callable task) {
        bufferPoolExecutor.submit(task);
    }

    private class BufferThreadFactory implements ThreadFactory {
        private AtomicInteger serialID;


        public BufferThreadFactory() {
            super();
            serialID = new AtomicInteger(1);
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("HBuffer-" + serialID.getAndIncrement());
            return t;
        }
    }

    private class BufferRejectedExecutionHandler implements RejectedExecutionHandler {
        public BufferRejectedExecutionHandler() {
            super();
        }

        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            throw new RejectedExecutionException("HBuffer " + r.toString() +
                    " rejected from " +
                    e.toString());
        }
    }
}

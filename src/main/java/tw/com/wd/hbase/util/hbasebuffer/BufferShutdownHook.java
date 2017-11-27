package tw.com.wd.hbase.util.hbasebuffer;

import java.util.ArrayList;
import java.util.List;

public class BufferShutdownHook extends Thread {
    private static final List<IShutdownable> shutdownableList;

    static {
        shutdownableList = new ArrayList<IShutdownable>(1);
    }

    private static final class InstanceHolder {
        private static final BufferShutdownHook INSTANCE = new BufferShutdownHook();
    }

    private BufferShutdownHook() {
        super();
    }

    public static BufferShutdownHook getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public void addShutdownable(IShutdownable shutdownable) {
        if (shutdownable == null) {
            throw new NullPointerException("IShutdownable == null");
        }
        shutdownableList.add(shutdownable);
    }

    @Override
    public void run() {
        for (IShutdownable shutdownable : shutdownableList) {
            shutdownable.shutdown();
        }
    }
}

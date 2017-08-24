package tw.com.wd.hbasebuffer;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Properties;

public class HBaseBufferConf {
    private static final String CONF_FILE = "conf/hbuffer.conf";
    private static int BUFFER_SIZE;     // Size of HBaseBuffer
    private static long OFFER_TIMEOUT;  // Timeout for offer to queue in millisecond

    static {
        initDefaultValue();
        readFromSystem();
        readFromFile();
    }

    private static void initDefaultValue() {
        HBaseBufferConf.BUFFER_SIZE     = 10000;
        HBaseBufferConf.OFFER_TIMEOUT   = 500l;
    }

    private static void readFromSystem() {
        Field[] fields = HBaseBufferConf.class.getDeclaredFields();

        for (Field f : fields) {
            if (f.getName().equals("CONF_FILE")) {
                continue;
            }

            String propValueString = System.getProperty(f.getName());

            if (propValueString != null) {
                f.setAccessible(true);
                try {
                    f.set(null, convertToOriginType(f.getType(), propValueString));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void readFromFile() {
        String dir      = System.getProperty("user.dir");
        File confFile   = new File(dir + File.separator + CONF_FILE);

        if (confFile.exists() && confFile.isFile()) {
            Properties props = new Properties();
            try {
                props.load(new FileInputStream(confFile));

                for (Field f : HBaseBufferConf.class.getDeclaredFields()) {
                    if (f.getName().equals("CONF_FILE")) {
                        continue;
                    }

                    String propValueString = props.getProperty(f.getName());
                    f.setAccessible(true);
                    f.set(null, convertToOriginType(f.getType(), propValueString));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            return;
        }
    }

    private static Object convertToOriginType(Class<?> type, String propValueString) {
        if (int.class.isAssignableFrom(type)) {
            return Integer.parseInt(propValueString);
        } else if (long.class.isAssignableFrom(type)) {
            return Long.parseLong(propValueString);
        } else {
            return propValueString;
        }
    }

    public static int getBufferSize() {
        return BUFFER_SIZE;
    }

    public static long getOfferTimeout() {
        return OFFER_TIMEOUT;
    }
}

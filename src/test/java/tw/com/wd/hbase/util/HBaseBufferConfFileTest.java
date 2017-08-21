package tw.com.wd.hbase.util;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HBaseBufferConfFileTest {
    @Test
    public void testValueFromFile() throws Exception {
        String rootPath = System.getProperty("user.dir");
        File confDir    = new File(rootPath + File.separator + "conf");
        confDir.mkdir();

        File confFile = new File(rootPath + File.separator + "conf" + File.separator + "hbuffer.conf");
        confFile.createNewFile();

        FileOutputStream fos = new FileOutputStream(confFile);
        fos.write("BUFFER_SIZE = 3333\n".getBytes());
        fos.write("OFFER_TIMEOUT = 100\n".getBytes());
        fos.close();

        int bufferSize      = HBaseBufferConf.getBufferSize();
        long offerTimeout   = HBaseBufferConf.getOfferTimeout();

        assertThat(bufferSize, is(3333));
        assertThat(offerTimeout, is(100l));

        confFile.delete();
        confDir.delete();
    }
}

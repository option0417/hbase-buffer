package tw.com.wd.hbase.util;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HBaseBufferConfSystemTest {

    @Test
    public void testValueFromSystem() {
        System.setProperty("BUFFER_SIZE", String.valueOf(5000));
        System.setProperty("OFFER_TIMEOUT", String.valueOf(1000l));

        int bufferSize      = HBaseBufferConf.getBufferSize();
        long offerTimeout   = HBaseBufferConf.getOfferTimeout();

        assertThat(bufferSize, is(5000));
        assertThat(offerTimeout, is(1000l));
    }

}

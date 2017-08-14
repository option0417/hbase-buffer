package tw.com.wd.hbase.util;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class HBaseBufferConfDefaultTest {
    @Test
    public void testDefaultValue() {
        int bufferSize      = HBaseBufferConf.getBufferSize();
        long offerTimeout   = HBaseBufferConf.getOfferTimeout();

        assertThat(bufferSize, is(10000));
        assertThat(offerTimeout, is(300l));
    }
}

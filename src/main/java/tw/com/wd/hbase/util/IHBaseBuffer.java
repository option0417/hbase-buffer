package tw.com.wd.hbase.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Row;
import tw.com.wd.hbase.exception.HBufferException;

public interface IHBaseBuffer {
    /**
     * Put Row operation (ex. Put, Get, Append, Increment, etc.) to HBaseBuffer
     * @param hbaseOP Row
     * @param tbl TableName
     * @return true if succeeded or false if cant put
     * @throws HBufferException
     */
    public boolean put(Row hbaseOP, TableName tbl) throws HBufferException;

    /**
     * Flush Rows in buffer to HBase
     * @throws HBufferException
     */
    public void flush() throws HBufferException;
}

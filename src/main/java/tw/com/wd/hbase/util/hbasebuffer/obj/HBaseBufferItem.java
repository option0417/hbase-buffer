package tw.com.wd.hbase.util.hbasebuffer.obj;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Row;

import java.util.Arrays;

public class HBaseBufferItem {
    private final Row hbaseOp;
    private final TableName tblName;


    public HBaseBufferItem(Row hbaseOp, TableName tblName) {
        this.hbaseOp = hbaseOp;
        this.tblName = tblName;
    }

    public Row getHbaseOp() {
        return hbaseOp;
    }

    public TableName getTblName() {
        return tblName;
    }

    @Override
    public String toString() {
        return "{\"TableName\":\""+ tblName.getNameAsString() + "\",\"Row\":\"" + hbaseOp.getClass().getSimpleName() + "\"}";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (HBaseBufferItem.class.isInstance(obj)) {
            HBaseBufferItem tmpItem = (HBaseBufferItem) obj;

            if (this.tblName.getNameAsString().equals(tmpItem.tblName.getNameAsString())) {
                if (Arrays.equals(this.hbaseOp.getRow(), tmpItem.hbaseOp.getRow())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        int hash = 31;

        byte[] tblNameBytes = this.tblName.getName();
        for (byte b : tblNameBytes) {
            hash += (int)b;
        }

        byte[] rowBytes = this.hbaseOp.getRow();
        for (byte b : rowBytes) {
            hash += (int)b;
        }

        return hash;
    }
}

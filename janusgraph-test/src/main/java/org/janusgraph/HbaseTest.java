package org.janusgraph;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class HbaseTest {
    static Logger logger = LoggerFactory.getLogger(HbaseTest.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        Connection conn = ConnectionFactory.createConnection(conf);
        String tableName = "janus_gods2";
        Table table = conn.getTable(TableName.valueOf(tableName));
        Iterator<Result> scanner = table.getScanner(new Scan()).iterator();
        int i = 0;
        while (scanner.hasNext()) {
            i++;
            Result result = scanner.next();
            byte[] rowkey = result.getRow();

            String b = toStringBinary(rowkey);
            logger.info("{} rowkey: {}", i, Bytes.toStringBinary(rowkey));
            logger.info("{} rowkey byte {} {}", i, rowkey.length, b);


            int j = 0;
            for (Cell cell : result.rawCells()) {
                j++;
                byte[] f = Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                byte[] q = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                byte[] v = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if (v.length == 8) {
                    if (4112 == Bytes.toLong(v)) {
                        logger.info("可能存储的复合索引  value=4112");
                    }
                }

                logger.info("{} family: {} qualifier: {} value: {}", i, Bytes.toString(f), Bytes.toString(q), Bytes.toString(v));
                logger.info("{} family: {} qualifier: {} value: {}", i, Bytes.toStringBinary(f), Bytes.toStringBinary(q), Bytes.toStringBinary(v));
                logger.info("{} qualifier: {}", i, toStringBinary(q));

                if (rowkey.length == 8 && b.endsWith("000") && j > 2) {
                    if (v.length > 1) {

//                        String vS = new StringSerializer().read(new ReadArrayBuffer(v));
//                        logger.info("vS: {}", vS);
                    }
                }
            }
            logger.info("{} {}\n", i, result);

        }
        conn.close();
    }

    public static String toStringBinary(byte[] bs) {
        if (bs == null || bs.length == 0) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        for (byte b : bs) {
            result.append(toStringBinary(b) + "-");
        }
        result.deleteCharAt(result.length() - 1);

        return result.toString();
    }

    public static String toStringBinary(byte b) {
        return Integer.toBinaryString((b & 0xFF) + 0x100).substring(1);
    }
}

package org.janusgraph;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

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

            logger.info("{} {}", i, Bytes.toStringBinary(rowkey));
            logger.info("{} {}", i, toStringBinary(rowkey));

            if (i==33){
                logger.info("");
            }
            for (Cell cell : result.rawCells()) {
                byte[] f = cell.getFamilyArray();
                byte[] q = cell.getQualifierArray();
                byte[] v = cell.getValueArray();
                logger.info("{} family: {} qualifier: {} value: {}", i, Bytes.toString(f), Bytes.toString(q), Bytes.toString(v));
            }
            logger.info("{} {}", i, result);

        }
        conn.close();
    }

    public static String toStringBinary(byte[] bs) {
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

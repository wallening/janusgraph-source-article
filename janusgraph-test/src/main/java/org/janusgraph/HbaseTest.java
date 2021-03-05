package org.janusgraph;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class HbaseTest {
    static Logger logger = LoggerFactory.getLogger(HbaseTest.class);

    public static void main(String[] args) throws IOException {

        try (Connection conn = getHbase()) {
            Table table = getTable(conn);


            test1_scan(table);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static {

    }

    public static Table getTable(Connection conn) throws IOException {
        Properties p = getProperties();
        String tableName = p.getProperty("storage.hbase.table");
        return conn.getTable(TableName.valueOf(tableName));
    }


    public static Connection getHbase() throws IOException {
        Properties p = getProperties();
        String zookeeper = p.getProperty("storage.hostname");


        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", zookeeper);

        return ConnectionFactory.createConnection(conf);
    }

    static void test2_deleteCompositeIndex(Table table) {
        try (JanusGraph client = JanusTest.getClient()) {

//            client.openManagement().getGraphIndex()


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static String identifier = null;

    static void test1_scan(Table table) throws IOException {
        Iterator<Result> scanner = table.getScanner(new Scan()).iterator();
        int i = 0;

        while (scanner.hasNext()) {
            Result result = scanner.next();
            byte[] rowkey = result.getRow();

            identifier = String.format("row: %3d", ++i);
            logger.info("{} rowkey_byte:   {} byteNum: {}", identifier, Bytes.toStringBinary(rowkey), rowkey.length);
            logger.info("{} rowkey_binary: {} ", identifier, toBinaryString(rowkey));

//            if (rowkey.length == 8) {
//                logger.info("{} id {}", identifier, VariableLong.read(new ReadArrayBuffer(rowkey)));
//            }

            int j = 0;
            for (Cell cell : result.rawCells()) {
                j++;
                byte[] f = Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                byte[] q = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                byte[] v = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                String family = Bytes.toString(f);

                logger.info("{} family: {} qualifier: {} value: {}", identifier, family, Bytes.toString(q), Bytes.toString(v));
                logger.info("{} family: {} qualifier: {} value: {}", identifier, family, Bytes.toStringBinary(q), Bytes.toStringBinary(v));
                logger.info("{} qualifier: {}", identifier, toBinaryString(q));


                if (family.equals("g")) {
                    testCompositeIndex(v);
                }

//                long typeid = parseQ(q);
                // 点属性p1 p2
//                if (typeid == 1029 || typeid == 2053) {
//                    parseV(v);
//
//                }

            }
            logger.info("{} {}\n", identifier, result);
        }
    }

    static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(JanusTest.p));
        return properties;
    }

    static void testCompositeIndex(byte[] v) {
        long id = VariableLong.readPositive(new ReadArrayBuffer(v));
        logger.info("{} 复合索引存储的id: {}", identifier, id);
    }

    static void parseV(byte[] v) {
        String vS = new StringSerializer().read(new ReadArrayBuffer(v));
        logger.info("value解析值: {}", vS);
    }

    static long parseQ(byte[] q) {
        try {
            if (q.length == 1 && q[0] == 0) {
                return -1;
            }
            IDHandler.RelationTypeParse typeAndDir = IDHandler.readRelationType(new ReadArrayBuffer(q));
            logger.info("属性或边id: {}", typeAndDir.typeId);
            return typeAndDir.typeId;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static String toBinaryString(byte[] bs) {
        if (bs == null || bs.length == 0) {
            return "";
        }

        String delimiter = "-";
        StringBuilder result = new StringBuilder();

        for (byte b : bs) {
            result.append(toBinaryString(b) + delimiter);
        }
        result.deleteCharAt(result.length() - 1);

        return result.toString();
    }

    public static String toBinaryString(byte b) {
        return Integer.toBinaryString((b & 0xFF) + 0x100).substring(1);
    }
}

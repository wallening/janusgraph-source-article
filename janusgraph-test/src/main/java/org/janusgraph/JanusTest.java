package org.janusgraph;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.hbase.ConnectionMask;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.diskstorage.hbase.HTable1_0;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import org.janusgraph.util.stats.NumberUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS;

public class JanusTest {
    static Logger logger = LoggerFactory.getLogger(JanusTest.class);
    public static String p = "./janusgraph-test/janusgraph-hbase-es.properties";

    public static void main(String[] args) {

        try (JanusGraph client = getClient()) {

//            test1_schema(client);

//            test2_data(client);
//            test3_queryV(client, v1, p1);

//            test4_printSchema(client);

            test5_VertexCompositeIndex((StandardJanusGraph) client);

//            test6_queryE(client, e1, p1);

//            test7_delete(client);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static void test7_delete(JanusGraph client) {
        logger.info("{} 清楚全部数据", Thread.currentThread().getStackTrace()[1].getMethodName());

        client.traversal().V().bothE().drop().iterate();
        client.traversal().V().drop().iterate();
        client.tx().commit();
    }

    static void test6_queryE(JanusGraph client, String label, String property) {
        String log = String.format("查询边测试 label: %s property: %s", label, property);
        logger.info("{}", log);
        List<Edge> list = client.traversal().E().hasLabel(label).has(property, value(label, property)).toList();
        for (Edge edge : list) {
            logger.info("{} id: {}", log, edge.id());

            for (String key : edge.keys()) {
                logger.info("{} property: {}={}", log, key, edge.property(key).value());
            }
        }
    }

    static void test5_VertexCompositeIndex(StandardJanusGraph client) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException, IOException {
        String indexName = indexName(v1, p1);
        String value = value(v1, p1);

        deleteEmptyVertexCompositeIndex(client, indexName, value);
    }

    public static HTable getHtable(StandardJanusGraph client) throws NoSuchFieldException, IllegalAccessException, IOException {
        HBaseStoreManager storeManager = (HBaseStoreManager) client.getBackend().getStoreManager();

        // 暴力破解私有参数
        Field tableNameField = HBaseStoreManager.class.getDeclaredField("tableName");
        tableNameField.setAccessible(true);
        String tableName = (String) tableNameField.get(storeManager);

        Field field = HBaseStoreManager.class.getDeclaredField("cnx");
        field.setAccessible(true);
        ConnectionMask cnx = (ConnectionMask) field.get(storeManager);
        HTable1_0 table1_0 = (HTable1_0) cnx.getTable(tableName);

        Field tableField = HTable1_0.class.getDeclaredField("table");
        tableField.setAccessible(true);
        HTable hTable = (HTable) tableField.get(table1_0);
        return hTable;
    }

    /**
     * 通过复合索引名和值获取rowkey
     * @param client
     * @param indexName
     * @param value
     * @return
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public static byte[] getCompositeIndexRowkey(StandardJanusGraph client, String indexName, String value) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ManagementSystem mgmt = (ManagementSystem) client.openManagement();
        CompositeIndexTypeWrapper indexType = (CompositeIndexTypeWrapper) ManagementSystem.getGraphIndexDirect(indexName, mgmt.getWrappedTx());

        assert indexType.isCompositeIndex();

        // 此方法是私有的,暴力破解
        IndexSerializer indexSerializer = client.getIndexSerializer();
        Method met = IndexSerializer.class.getDeclaredMethod("getIndexKey", CompositeIndexType.class, Object[].class);
        met.setAccessible(true);
        StaticBuffer buffer = (StaticBuffer) met.invoke(indexSerializer, indexType, new Object[]{value});

        return buffer.asByteBuffer().array();
    }

    /**
     * 通过点id获取rowkey
     * @param client
     * @param vertexId
     * @return
     */
    public static byte[] getVertexRowkey(StandardJanusGraph client, long vertexId) {
        int partitionBits = NumberUtil.getPowerOf2(client.getConfiguration().getConfiguration().get(CLUSTER_MAX_PARTITIONS));
        IDManager idManager = new IDManager(partitionBits);

        // id 序列化
        byte[] vertexRowkey = idManager.getKey(vertexId).asByteBuffer().array();
        long id2 = idManager.getKeyID(new ReadArrayBuffer(vertexRowkey));

        assert vertexId == id2;

        return vertexRowkey;
    }

    public static void deleteEmptyVertexCompositeIndex(StandardJanusGraph client, String indexName, String value) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException, IOException {
        String identifier = String.format("删除[点]失效的复合索引 ==> indexName: %s value: %s", indexName, value);

        HTable hTable = getHtable(client);
        byte[] indexRowkey = getCompositeIndexRowkey(client, indexName, value);
        Result result = hTable.get(new Get(indexRowkey));

        logger.info("{} rowkey: {}", identifier, Bytes.toStringBinary(indexRowkey));

        for (Cell cell : result.rawCells()) {
            byte[] f = Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            byte[] q = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            byte[] v = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            long vertexId = VariableLong.readPositive(new ReadArrayBuffer(v));
            byte[] vertexRowkey = getVertexRowkey(client, vertexId);

            Result result2 = hTable.get(new Get(vertexRowkey));
            vertexRowkey = result2.getRow();

            if (vertexRowkey == null) {
                logger.info("{} vertexId: {} 点不存在，删除相关索引信息", identifier, vertexId);
                Delete delete = new Delete(indexRowkey).addColumn(f, q);
                hTable.delete(delete);
            }
        }
    }

    static void test4_printSchema(JanusGraph client) {
        logger.info("{} 获取schema", Thread.currentThread().getStackTrace()[1].getMethodName());
        ManagementSystem mgmt = (ManagementSystem) client.openManagement();
        String s = mgmt.printSchema();
        System.out.println(s);


//        Iterable<JanusGraphIndex> vertexIndexes = mgmt.getGraphIndexes(Vertex.class);
//        for (JanusGraphIndex vertexIndex : vertexIndexes) {
//            CompositeIndexTypeWrapper indexType = (CompositeIndexTypeWrapper) mgmt.getGraphIndexDirect(vertexIndex.name(), mgmt.getWrappedTx());
//            SchemaSource schemaSource = indexType.getSchemaBase();
//            long id = indexType.getID();
//            byte[] bytes = Bytes.toBytes(id);
//            logger.info("indexName: {} id: {} {}", vertexIndex.name(), id, HbaseTest.toBinaryString(bytes));
//        }


        PropertyKey pp1 = mgmt.getPropertyKey(p1);
        PropertyKey pp2 = mgmt.getPropertyKey(p2);

        VertexLabel vv1 = mgmt.getVertexLabel(v1);
        VertexLabel vv2 = mgmt.getVertexLabel(v2);
        EdgeLabel ee1 = mgmt.getEdgeLabel(e1);

        logger.info("PropertyKey: {} id: {} {}", p1, pp1.longId(), HbaseTest.toBinaryString(Bytes.toBytes(pp1.longId())));
        logger.info("PropertyKey: {} id: {} {}", p2, pp2.longId(), HbaseTest.toBinaryString(Bytes.toBytes(pp2.longId())));

        logger.info("VertexLabel: {} id: {} {}", v1, vv1.longId(), HbaseTest.toBinaryString(Bytes.toBytes(vv1.longId())));
        logger.info("VertexLabel: {} id: {} {}", v2, vv2.longId(), HbaseTest.toBinaryString(Bytes.toBytes(vv2.longId())));
        logger.info("EdgeLabel: {} id: {} {}", e1, ee1.longId(), HbaseTest.toBinaryString(Bytes.toBytes(ee1.longId())));

        List<Vertex> vs = client.traversal().V().toList();
        for (Vertex v : vs) {
            logger.info("Vertex id: {}", v.id());
        }

        List<Edge> es = client.traversal().E().toList();
        for (Edge e : es) {
            RelationIdentifier ri = ((RelationIdentifier) e.id());
            logger.info("Edge id: {} outV: {} inV: {}", ri.getRelationId(), ri.getOutVertexId(), ri.getInVertexId());
        }
    }

    static void test3_queryV(JanusGraph client, String label, String property) {
        logger.info("{} 点查询", Thread.currentThread().getStackTrace()[1].getMethodName());
        GraphTraversal<Vertex, Vertex> it = client.traversal().V().hasLabel(label).has(property, value(label, property));
        List<Vertex> list = it.toList();

        String log = String.format("查询点测试 label: %s property: %s result: %s", label, property, list.size());
        logger.info("{}", log);

        for (Vertex vertex : list) {
            logger.info("{} id: {} {}", log, vertex.id(), HbaseTest.toBinaryString(Bytes.toBytes((Long) vertex.id())));

            for (String key : vertex.keys()) {
                logger.info("{} property: {}={}", log, key, vertex.property(key).value());
            }
        }

//        try {
//            TimeUnit.SECONDS.sleep(10);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }


    static void test2_data(JanusGraph client) {
        logger.info("{} 插入数据", Thread.currentThread().getStackTrace()[1].getMethodName());
        JanusGraphTransaction tx = client.newTransaction();

        Vertex vv1 = tx.addVertex(T.label, v1, p1, value(v1, p1), p2, value(v1, p2));
        Vertex vv2 = tx.addVertex(T.label, v2, p1, value(v2, p1), p2, value(v2, p2));
        vv1.addEdge(e1, vv2, p1, value(e1, p1), p2, value(e1, p2));

        tx.commit();
    }

    static String v1 = "v1";
    static String v2 = "v2";

    static String p1 = "p1";
    static String p2 = "p2";

    static String e1 = "e1";

    static String iv1 = indexName(v1, p1);
    static String iv2 = indexName(v2, p1);
    static String ie1 = indexName(e1, p1);

    static String indexName(String label, String property) {
        return label + "_" + property;
    }

    static String value(String label, String property) {
        return String.format("value[%s:%s]", label, property);
    }

    static void test1_schema(JanusGraph client) {
        logger.info("{} 创建schema", Thread.currentThread().getStackTrace()[1].getMethodName());
        JanusGraphManagement mgmt = client.openManagement();

        PropertyKey pp1 = mgmt.makePropertyKey(p1).dataType(String.class).make();
        PropertyKey pp2 = mgmt.makePropertyKey(p2).dataType(String.class).make();

        VertexLabel vv1 = mgmt.makeVertexLabel(v1).make();
        VertexLabel vv2 = mgmt.makeVertexLabel(v2).make();

        EdgeLabel ee1 = mgmt.makeEdgeLabel(e1).multiplicity(Multiplicity.MULTI).make();

        mgmt.buildIndex(iv1, Vertex.class).addKey(pp1).indexOnly(vv1).buildCompositeIndex();
        mgmt.buildIndex(iv2, Vertex.class).addKey(pp1).indexOnly(vv2).buildCompositeIndex();
        mgmt.buildIndex(ie1, Edge.class).addKey(pp1).indexOnly(ee1).buildCompositeIndex();

        mgmt.commit();
    }


    public static JanusGraph getClient() throws ConfigurationException {
        return JanusGraphFactory.open(new PropertiesConfiguration(p));
    }
}

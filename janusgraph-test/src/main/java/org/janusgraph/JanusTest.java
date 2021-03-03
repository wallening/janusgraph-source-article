package org.janusgraph;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.hbase.HBaseKeyColumnValueStore;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.JanusGraphIndexWrapper;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JanusTest {
    static Logger logger = LoggerFactory.getLogger(JanusTest.class);

    public static void main(String[] args) {
        String p = "./janusgraph-test/janusgraph-hbase-es.properties";
        try (JanusGraph client = JanusGraphFactory.open(new PropertiesConfiguration(p))) {

//            test1_schema(client);

//            test2_data(client);

            test3_query(client);

//            test4_printSchema(client);

//            test5_compositeindex((StandardJanusGraph) client);

//            test6_queryE(client, p1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static void test6_queryE(JanusGraph client, String val) {
        String log = String.format("查询边测试 %s", val);
        logger.info("{}", log);
        List<Edge> list = client.traversal().V().hasLabel(v1).has(p1, val).bothE().toList();
        for (Edge edge : list) {
            logger.info("{} id: {} {}", log, edge.id(), HbaseTest.toStringBinary(Bytes.toBytes((Long) edge.id())));

            for (String key : edge.keys()) {
                logger.info("{} property: {}={}", log, key, edge.property(key).value());
            }
        }

//        try {
//            TimeUnit.SECONDS.sleep(10);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    static void test5_compositeindex(StandardJanusGraph client) {
        String indexName = "v1_p1";
        String value = "v1p1";
        IndexSerializer indexSerializer = client.getIndexSerializer();
        ManagementSystem mgmt = (ManagementSystem) client.openManagement();
        CompositeIndexTypeWrapper indexType = (CompositeIndexTypeWrapper) ManagementSystem.getGraphIndexDirect(indexName, mgmt.getWrappedTx());
        Long id = indexType.getID();

        logger.info("复合索引 {} id: {}", indexName, id);
        Object[] objs = new Object[1];
        objs[0] = value;
        // 此方法是私有的
        StaticBuffer buffer = indexSerializer.getIndexKey(indexType, objs);
        byte[] bs = buffer.asByteBuffer().array();
        String rowkey = HbaseTest.toStringBinary(bs);

        logger.info("复合索引 {} rowkey: {}", indexName, rowkey);
        logger.info("复合索引 {} rowkey: {}", indexName, Bytes.toStringBinary(bs));

        try {
            HBaseStoreManager b = (HBaseStoreManager) client.getBackend().getStoreManager();
            HBaseKeyColumnValueStore d = (HBaseKeyColumnValueStore) b.openDatabase(b.getName());

            System.out.println("---");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void test4_printSchema(JanusGraph client) {
        ManagementSystem mgmt = (ManagementSystem) client.openManagement();
        String s = mgmt.printSchema();
        System.out.println(s);


//        Iterable<JanusGraphIndex> vertexIndexes = mgmt.getGraphIndexes(Vertex.class);
//        for (JanusGraphIndex vertexIndex : vertexIndexes) {
//            CompositeIndexTypeWrapper indexType = (CompositeIndexTypeWrapper) mgmt.getGraphIndexDirect(vertexIndex.name(), mgmt.getWrappedTx());
//            SchemaSource schemaSource = indexType.getSchemaBase();
//            long id = indexType.getID();
//            byte[] bytes = Bytes.toBytes(id);
//            logger.info("indexName: {} id: {} {}", vertexIndex.name(), id, HbaseTest.toStringBinary(bytes));
//        }

        PropertyKey pp1 = mgmt.getPropertyKey(p1);
        PropertyKey pp2 = mgmt.getPropertyKey(p2);

        VertexLabel vv1 = mgmt.getVertexLabel(v1);
        VertexLabel vv2 = mgmt.getVertexLabel(v2);
        EdgeLabel ee1 = mgmt.getEdgeLabel(e1);

        logger.info("PropertyKey: {} id: {} {}", p1, pp1.longId(), HbaseTest.toStringBinary(Bytes.toBytes(pp1.longId())));
        logger.info("PropertyKey: {} id: {} {}", p2, pp2.longId(), HbaseTest.toStringBinary(Bytes.toBytes(pp2.longId())));

        logger.info("VertexLabel: {} id: {} {}", v1, vv1.longId(), HbaseTest.toStringBinary(Bytes.toBytes(vv1.longId())));
        logger.info("VertexLabel: {} id: {} {}", v2, vv2.longId(), HbaseTest.toStringBinary(Bytes.toBytes(vv2.longId())));
        logger.info("EdgeLabel: {} id: {} {}", e1, ee1.longId(), HbaseTest.toStringBinary(Bytes.toBytes(ee1.longId())));
    }

    static void test3_query(JanusGraph client) {
        query(client, "v1p1");
//        query(client, "v1p1");
//        query(client, "v1p2");
//        query(client, "v1p1");
    }

    static void query(JanusGraph client, String val) {
        String log = String.format("查询点测试 %s", val);
        logger.info("{}", log);
        List<Vertex> list = client.traversal().V().hasLabel(v1).has(p1, val).toList();
        for (Vertex vertex : list) {
            logger.info("{} id: {} {}", log, vertex.id(), HbaseTest.toStringBinary(Bytes.toBytes((Long) vertex.id())));

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
        JanusGraphTransaction tx = client.newTransaction();

        Vertex vv1 = tx.addVertex(T.label, v1, p1, "v1p1", p2, "v1p2");
        Vertex vv2 = tx.addVertex(T.label, v2, p1, "v2p1", p2, "v2p2");
        vv1.addEdge(e1, vv2, p1, "e1p1", p2, "e2p2");

        tx.commit();
    }

    static String v1 = "v1";
    static String v2 = "v2";

    static String p1 = "p1";
    static String p2 = "p2";

    static String e1 = "e1";

    static String iv1 = v1 + "_" + p1;
    static String iv2 = v2 + "_" + p1;
    static String ie1 = e1 + "_" + p1;

    static void test1_schema(JanusGraph client) {
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
}

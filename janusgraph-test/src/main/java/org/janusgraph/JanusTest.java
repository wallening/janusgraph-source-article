package org.janusgraph;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JanusTest {
    static Logger logger = LoggerFactory.getLogger(JanusTest.class);

    public static void main(String[] args) {
        String p = "/home/sdzw/local/workspace/JanusGraph/janusgraph-source-article/janusgraph-test/janusgraph-hbase-es.properties";
        try (JanusGraph client = JanusGraphFactory.open(new PropertiesConfiguration(p))) {

//            test1_schema(client);

//            test2_data(client);

            test3_query(client);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static void test3_query(JanusGraph client) {
        List<Vertex> list = client.traversal().V().hasLabel(v1).has(p1, "v1p1").toList();
        for (Vertex vertex : list) {
            logger.info("id: {}", vertex.id());

            for (String key : vertex.keys()) {
                logger.info("property: {}={}", key, vertex.property(key).value());
            }
        }
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

    static void test1_schema(JanusGraph client) {
        JanusGraphManagement mgmt = client.openManagement();

        PropertyKey pp1 = mgmt.makePropertyKey(p1).dataType(String.class).make();
        PropertyKey pp2 = mgmt.makePropertyKey(p2).dataType(String.class).make();

        VertexLabel vv1 = mgmt.makeVertexLabel(v1).make();
        VertexLabel vv2 = mgmt.makeVertexLabel(v2).make();

        EdgeLabel ee1 = mgmt.makeEdgeLabel(e1).multiplicity(Multiplicity.MULTI).make();

        mgmt.buildIndex(v1 + "_" + p1, Vertex.class).addKey(pp1).indexOnly(vv1).buildCompositeIndex();
        mgmt.buildIndex(v2 + "_" + p1, Vertex.class).addKey(pp1).indexOnly(vv2).buildCompositeIndex();
        mgmt.buildIndex(e1 + "_" + p1, Edge.class).addKey(pp1).indexOnly(ee1).buildCompositeIndex();

        mgmt.commit();
    }
}

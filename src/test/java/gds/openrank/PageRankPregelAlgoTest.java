package gds.openrank;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;

import static org.neo4j.gds.TestSupport.assertDoubleValues;

@GdlExtension
class PageRankPregelAlgoTest {

    @GdlGraph
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (u1:User{retentionFactor:0.5,initValue:1.0})" +
            ", (u2:User{retentionFactor:0.5,initValue:1.5})" +
            ", (u3:User{retentionFactor:0.5,initValue:0.5})" +
            ", (r1:Repo{retentionFactor:0.3,initValue:0.3})" +
            ", (r2:Repo{retentionFactor:0.3,initValue:0.7})" +
            ", (r3:Repo{retentionFactor:0.3,initValue:2.0})" +
            ", (u1)-[:REL{weight:0.05}]->(u2)" +
            ", (u1)-[:REL{weight:0.05}]->(u3)" +
            ", (u3)-[:REL{weight:0.1}]->(u2)" +
            ", (r1)-[:REL{weight:0.1}]->(r2)" +
            ", (r3)-[:REL{weight:0.1}]->(r2)" +
            ", (u1)-[:REL{weight:0.27}]->(r1)" +
            ", (r1)-[:REL{weight:0.9}]->(u1)" +
            ", (u1)-[:REL{weight:0.63}]->(r3)" +
            ", (r3)-[:REL{weight:0.63}]->(u1)" +
            ", (u2)-[:REL{weight:0.54}]->(r2)" +
            ", (r2)-[:REL{weight:0.54}]->(u2)" +
            ", (u2)-[:REL{weight:0.36}]->(r3)" +
            ", (r3)-[:REL{weight:0.27}]->(u2)" +
            ", (u3)-[:REL{weight:0.9}]->(r2)" +
            ", (r2)-[:REL{weight:0.36}]->(u3)";

    @Inject
    private TestGraph graph;

    @Test
    void runPR() {
        int maxIterations = 30;

        var config = ImmutableOpenRankPregelConfig.builder()
            .maxIterations(maxIterations)
            .isAsynchronous(false)
            .initValueProperty("initValue")
            .retentionFactorProperty("retentionFactor")
            .relationshipWeightProperty("weight")
            .tolerance(0.0001d)
            .enableLog(true)
            .suffix("_1")
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new OpenRankPregel(),
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        );

        HugeDoubleArray nodeValues = pregelJob.run().nodeValues().doubleProperties("open_rank_1");

        // verify the result by algorithm [(E-AS)^-1](E-A)v^(0)
        var expected = new HashMap<String, Double>();
        expected.put("u1", 1.072D);
        expected.put("u2", 1.291D);
        expected.put("u3", 0.478D);
        expected.put("r1", 0.293D);
        expected.put("r2", 1.118D);
        expected.put("r3", 1.398D);

        assertDoubleValues(graph, nodeValues::get, expected, 1E-3);
    }
}

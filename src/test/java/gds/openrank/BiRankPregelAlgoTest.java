package gds.openrank;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class BiRankPregelAlgoTest {

    @GdlGraph
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (u1:User{retentionFactor:0.5,initValue:0.0})" +
            ", (u2:User{retentionFactor:0.5,initValue:0.0})" +
            ", (u3:User{retentionFactor:0.5,initValue:0.0})" +
            ", (r1:Repo{retentionFactor:0.3,initValue:1.0})" +
            ", (r2:Repo{retentionFactor:0.3,initValue:1.0})" +
            ", (r3:Repo{retentionFactor:0.3,initValue:1.0})" +
            ", (u1)-[:REL{weight:0.474341649}]->(r2)" + // 3
            ", (u1)-[:REL{weight:0.510310363}]->(r3)" + // 5
            ", (u2)-[:REL{weight:0.8660254}]->(r1)" +   // 6
            ", (u2)-[:REL{weight:0.316227766}]->(r2)" + // 2
            ", (u3)-[:REL{weight:0.7637626158}]->(r3)" +    // 7
            ", (u1)<-[:REL{weight:0.474341649}]-(r2)" +
            ", (u1)<-[:REL{weight:0.510310363}]-(r3)" +
            ", (u2)<-[:REL{weight:0.8660254}]-(r1)" +
            ", (u2)<-[:REL{weight:0.316227766}]-(r2)" +
            ", (u3)<-[:REL{weight:0.7637626158}]-(r3)";

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
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();
        assertTrue(result.didConverge(), "BiRank did not converge.");
    }
}

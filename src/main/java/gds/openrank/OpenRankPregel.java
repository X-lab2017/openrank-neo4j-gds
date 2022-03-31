package gds.openrank;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.PregelSchema.Visibility;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;
import org.neo4j.gds.config.GraphCreateConfig;

import java.util.Optional;
import java.util.function.LongPredicate;

@PregelProcedure(
    name = "xlab.pregel.openrank",
    modes = {GDSMode.STREAM, GDSMode.WRITE},
    description = "X-lab OpenRank in Pregel implementation"
)
public class OpenRankPregel implements PregelComputation<OpenRankPregel.OpenRankPregelConfig> {

    private String openRank = "open_rank";
    static String initValue = "init_value";
    static String rententionFactor = "retention_factor";
    static String converged = "converged";
    private boolean enableLog;
    private double tolerance;

    private void log(String s, Object ...args) {
        if (this.enableLog) {
            System.out.println(String.format(s, args));
        }
    }

    @Override
    public PregelSchema schema(OpenRankPregelConfig config) {
        this.tolerance = config.tolerance();
        this.enableLog = config.enableLog();
        this.openRank += config.suffix();
        return new PregelSchema.Builder()
            .add(openRank, ValueType.DOUBLE)
            .add(initValue, ValueType.DOUBLE, Visibility.PRIVATE)
            .add(rententionFactor, ValueType.DOUBLE, Visibility.PRIVATE)
            .add(converged, ValueType.LONG, Visibility.PRIVATE)
            .build();
    }

    @Override
    public void init(InitContext<OpenRankPregelConfig> context) {
        if (context.nodePropertyKeys().contains(context.config().initValueProperty())) {
            double initValueDouble = context.nodeProperties(context.config().initValueProperty()).doubleValue(context.nodeId());
            context.setNodeValue(initValue, initValueDouble);
            context.setNodeValue(openRank, initValueDouble);
        } else {
            context.setNodeValue(initValue, 1d);
            context.setNodeValue(openRank, 1d);
        }

        if (context.nodePropertyKeys().contains(context.config().retentionFactorProperty())) {
            double retentionFactor = context.nodeProperties(context.config().retentionFactorProperty()).doubleValue(context.nodeId());
            context.setNodeValue(rententionFactor, retentionFactor);
        } else {
            context.setNodeValue(rententionFactor, 0.15);
        }
        context.setNodeValue(converged, 0l);
    }

    @Override
    public void compute(ComputeContext<OpenRankPregelConfig> context, Messages messages) {
        var initValueDouble = context.doubleNodeValue(initValue);
        var oldRank = context.doubleNodeValue(openRank);
        // skip calculation for converged nodes
        if (!context.isInitialSuperstep() && context.longNodeValue(converged) == 0l) {
            var sum = 0d;
            for (var message : messages) {
                sum += message;
            }

            var retentionFactor = context.doubleNodeValue(rententionFactor);
            var newRank = retentionFactor * initValueDouble + (1 - retentionFactor) * sum;

            // set new value and convergence state
            context.setNodeValue(openRank, newRank);
            if (Math.abs(newRank - oldRank) < this.tolerance) {
                context.setNodeValue(converged, 1l);
            }
        }
        context.sendToNeighbors(context.doubleNodeValue(openRank));
    }

    @Override
    public boolean masterCompute(MasterComputeContext<OpenRankPregelConfig> context) {
        var result = new ConvergeResult();
        context.forEachNode(new LongPredicate() {
            @Override
            public boolean test(long id) {
                if (context.longNodeValue(id, converged) == 0l) {
                    result.isConverged = false;
                    return true;
                }
                return false;
            }
        });
        if (result.isConverged) {
            log("The process converged at iteration %d", context.superstep());
        }
        return result.isConverged;
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer.Sum());
    }

    @Override
    public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue * relationshipWeight;
    }

    @ValueClass
    @Configuration("OpenRankPregelConfigImpl")
    @SuppressWarnings("immutables:subtype")
    public interface OpenRankPregelConfig extends PregelProcedureConfig {
        @Value.Default
        default String retentionFactorProperty() {
            return "RETENTION_FACTOR";
        }

        @Value.Default
        default String initValueProperty() {
            return "INIT_VALUE";
        }

        @Value.Default
        default double tolerance() {
            return 0.001d;
        }

        @Value.Default
        default boolean enableLog() {
            return false;
        }

        @Value.Default
        default String suffix() {
            return "";
        }

        static OpenRankPregelConfig of(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper userInput
        ) {
            return new OpenRankPregelConfigImpl(graphName, maybeImplicitCreate, username, userInput);
        }
    }

    private class ConvergeResult {
        public boolean isConverged;

        public ConvergeResult() {
            isConverged = true;
        }
    }
}

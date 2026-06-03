/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.AggregateFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * DataFusion-side {@link AggregateFunctionAdapter}s rewriting PPL-form aggregate calls
 * (the {@code stats … by} GROUP BY path) into shapes the substrait emitter knows:
 * <ul>
 *   <li>{@link #argMin()} / {@link #argMax()} — {@code ARG_MIN/MAX(value, ts)} →
 *       {@code first_value/last_value(value) ORDER BY ts ASC} via the
 *       {@code LOCAL_FIRST_OP} / {@code LOCAL_LAST_OP} SqlAggFunctions defined in
 *       {@link DataFusionFragmentConvertor} (DataFusion 53.x has no native arg_min/max
 *       aggregate UDAF, but the ordered-aggregate form is equivalent).</li>
 *   <li>{@link #distinctCountApprox()} — {@code DISTINCT_COUNT_APPROX(x)} → Calcite
 *       {@code APPROX_COUNT_DISTINCT(x)}, which the convertor renames to substrait
 *       {@code approx_distinct} (DataFusion's built-in HLL).</li>
 * </ul>
 *
 * Mirrors {@link WindowFunctionAdapters} for the GROUP BY aggregate path.
 *
 * @opensearch.internal
 */
final class AggregateFunctionAdapters {

    private AggregateFunctionAdapters() {}

    static AggregateFunctionAdapter argMin() {
        return new ArgFunctionAdapter(DataFusionFragmentConvertor.LOCAL_FIRST_OP);
    }

    static AggregateFunctionAdapter argMax() {
        return new ArgFunctionAdapter(DataFusionFragmentConvertor.LOCAL_LAST_OP);
    }

    static AggregateFunctionAdapter distinctCountApprox() {
        return (original, argTypes, cluster) -> AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            original.isDistinct(),
            original.isApproximate(),
            original.ignoreNulls(),
            original.rexList,
            original.getArgList(),
            original.filterArg,
            original.distinctKeys,
            original.collation,
            original.getType(),
            original.getName()
        );
    }

    /** Rewrites {@code ARG_MIN(value, ts)} / {@code ARG_MAX(value, ts)} to {@code first_value(value)} /
     *  {@code last_value(value)} with {@code ts} appended as an ASC order key. The two-arg shape is
     *  the only one PPL emits today; falls back to a passthrough if the operand list is unexpected. */
    private record ArgFunctionAdapter(SqlAggFunction target) implements AggregateFunctionAdapter {
        @Override
        public AggregateCall adapt(AggregateCall original, List<RelDataType> argTypes, RelOptCluster cluster) {
            List<Integer> argList = original.getArgList();
            if (argList.size() != 2) {
                // Unexpected shape — preserve original behaviour.
                return original;
            }
            int valueIdx = argList.get(0);
            int tsIdx = argList.get(1);

            List<RelFieldCollation> collations = new ArrayList<>(original.collation.getFieldCollations());
            collations.add(new RelFieldCollation(tsIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));

            return AggregateCall.create(
                target,
                original.isDistinct(),
                original.isApproximate(),
                original.ignoreNulls(),
                original.rexList,
                ImmutableList.of(valueIdx),
                original.filterArg,
                original.distinctKeys,
                RelCollations.of(collations),
                original.getType(),
                original.getName()
            );
        }
    }
}

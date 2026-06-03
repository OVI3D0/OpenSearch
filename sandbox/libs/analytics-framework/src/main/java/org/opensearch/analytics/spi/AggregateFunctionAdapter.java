/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Per-function adapter that rewrites a backend-agnostic aggregate {@link AggregateCall}
 * into a backend-compatible form before substrait emission. Registered by backends keyed
 * by {@link AggregateFunction}. Argument-list indices arrive after any upstream
 * scalar-recursion pass; the adapter returns the bound {@link AggregateCall} that
 * {@code BackendPlanAdapter} feeds into the fragment converter.
 *
 * <p>Mirrors {@link WindowFunctionAdapter} for the GROUP BY aggregate path. PPL surfaces
 * functions like {@code earliest} / {@code latest} / {@code distinct_count_approx} as
 * {@link AggregateCall}s with {@code SqlKind.ARG_MIN} / {@code SqlKind.ARG_MAX} / a
 * {@code SqlOperator} named {@code DISTINCT_COUNT_APPROX}; backends that natively support
 * those (e.g. DataFusion) register adapters here to translate the {@link AggregateCall}
 * into the form their substrait emitter knows how to bind.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface AggregateFunctionAdapter {

    /**
     * Adapt the given {@link AggregateCall} for backend compatibility.
     *
     * @param original   the original aggregate call (use for argument list, return type, distinct flag)
     * @param argTypes   row types of the input expressions, indexed by {@code original.getArgList()}
     * @param cluster    provides {@code getRexBuilder()} / {@code getTypeFactory()} for adapter construction
     * @return the adapted call to emit in the plan
     */
    AggregateCall adapt(AggregateCall original, List<RelDataType> argTypes, RelOptCluster cluster);
}

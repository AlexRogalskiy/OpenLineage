/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.OVERWRITE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceCommand, OpenLineage.OutputDataset> {

  public InsertIntoDataSourceVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoDataSourceCommand command = (InsertIntoDataSourceCommand) x;

    return PlanUtils.applyFirst(
            context.getOutputDatasetQueryPlanVisitors(), command.logicalRelation())
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .peek(
            ds -> {
              Builder<String, OpenLineage.DatasetFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.DatasetFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              if (command.overwrite()) {
                facetsMap.put("tableStateChange", new TableStateChangeFacet(OVERWRITE));
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());
            })
        .collect(Collectors.toList());
  }
}

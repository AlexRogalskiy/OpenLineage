/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.OVERWRITE;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.IcebergHandler;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression;
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class TableContentChangeVisitor
    extends QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset> {

  public TableContentChangeVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return (x instanceof OverwriteByExpression)
        || (x instanceof OverwritePartitionsDynamic)
        || (x instanceof DeleteFromTable)
        || (x instanceof UpdateTable)
        || (new IcebergHandler().hasClasses() && x instanceof ReplaceData)
        || (x instanceof MergeIntoTable)
        || (x instanceof InsertIntoStatement);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    NamedRelation table;
    Map<String, OpenLineage.DatasetFacet> facetMap = new HashMap<>();

    // INSERT OVERWRITE TABLE SQL statement is translated into InsertIntoTable logical operator.
    if (x instanceof OverwriteByExpression) {
      table = ((OverwriteByExpression) x).table();
      includeOverwriteFacet(facetMap);
    } else if (x instanceof InsertIntoStatement) {
      table = (NamedRelation) ((InsertIntoStatement) x).table();
      if (((InsertIntoStatement) x).overwrite()) {
        includeOverwriteFacet(facetMap);
      }
    } else if (new IcebergHandler().hasClasses() && x instanceof ReplaceData) {
      // DELETE FROM on ICEBERG HAS START ELEMENT WITH ReplaceData AND COMPLETE ONE WITH
      // DeleteFromTable
      table = ((ReplaceData) x).table();
    } else if (x instanceof DeleteFromTable) {
      table = (NamedRelation) ((DeleteFromTable) x).table();
    } else if (x instanceof UpdateTable) {
      table = (NamedRelation) ((UpdateTable) x).table();
    } else if (x instanceof MergeIntoTable) {
      table = (NamedRelation) ((MergeIntoTable) x).targetTable();
    } else {
      table = ((OverwritePartitionsDynamic) x).table();
      includeOverwriteFacet(facetMap);
    }

    return PlanUtils3.fromDataSourceV2Relation(
        outputDataset(), context, (DataSourceV2Relation) table, facetMap);
  }

  private void includeOverwriteFacet(Map<String, OpenLineage.DatasetFacet> facetMap) {
    facetMap.put("tableStateChange", new TableStateChangeFacet(OVERWRITE));
  }
}

/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceDirCommand} and extracts
 * the output {@link OpenLineage.Dataset} being written.
 */
public class InsertIntoDataSourceDirVisitor
    extends QueryPlanVisitor<InsertIntoDataSourceDirCommand, OpenLineage.OutputDataset> {

  public InsertIntoDataSourceDirVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    InsertIntoDataSourceDirCommand command = (InsertIntoDataSourceDirCommand) x;
    // URI is required by the InsertIntoDataSourceDirCommand
    DatasetIdentifier di = PathUtils.fromURI(command.storage().locationUri().get(), "file");
    Map<String, OpenLineage.DatasetFacet> facets =
        command.overwrite()
            ? Collections.singletonMap(
                "tableStateChange", new TableStateChangeFacet(StateChange.OVERWRITE))
            : Collections.emptyMap();

    return Collections.singletonList(outputDataset().getDataset(di, command.schema(), facets));
  }
}

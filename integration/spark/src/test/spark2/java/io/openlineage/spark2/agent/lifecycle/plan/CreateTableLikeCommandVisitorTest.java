/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark2.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.immutable.HashMap;

class CreateTableLikeCommandVisitorTest {

  SparkSession session = mock(SparkSession.class);
  SessionCatalog sessionCatalog = mock(SessionCatalog.class);
  SessionState sessionState = mock(SessionState.class);
  Catalog catalog = mock(Catalog.class);
  CatalogTable catalogTable = mock(CatalogTable.class);

  String database = "default";
  TableIdentifier tableIdentifier = TableIdentifier$.MODULE$.apply("table", Option.apply(database));

  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    when(session.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn("default");
    when(session.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
  }

  @Test
  @SneakyThrows
  void testCreateTableLikeCommand() {
    when(sessionCatalog.defaultTablePath(tableIdentifier))
        .thenReturn(new URI("file://tmp/some-uri"));
    when(sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdentifier))
        .thenReturn(catalogTable);
    when(catalogTable.schema()).thenReturn(schema);

    CreateTableLikeCommandVisitor visitor =
        new CreateTableLikeCommandVisitor(SparkAgentTestExtension.newContext(session));

    CreateTableLikeCommand command =
        new CreateTableLikeCommand(
            tableIdentifier, tableIdentifier, Option.apply("/path/to/data"), false);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        new TableStateChangeFacet(CREATE),
        outputDataset.getFacets().getAdditionalProperties().get("tableStateChange"));
    assertEquals("/path/to/data", outputDataset.getName());
    assertEquals("file", outputDataset.getNamespace());
  }
}

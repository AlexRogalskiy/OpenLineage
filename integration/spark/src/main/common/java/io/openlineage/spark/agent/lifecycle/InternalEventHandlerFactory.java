/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.facets.builder.ErrorFacetBuilder;
import io.openlineage.spark.agent.facets.builder.LogicalPlanRunFacetBuilder;
import io.openlineage.spark.agent.facets.builder.OutputStatisticsOutputDatasetFacetBuilder;
import io.openlineage.spark.agent.facets.builder.SparkVersionFacetBuilder;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Internal implementation of the {@link OpenLineageEventHandlerFactory} interface. This
 * implementation is responsible for loading all implementations declared on the classpath (using
 * standard {@link ServiceLoader} conventions), as well as loading all internal plan and event
 * functions. No guarantees are made regarding the order in which {@link
 * OpenLineageEventHandlerFactory} components are loaded or stored.
 *
 * @see ServiceLoader documentation for guidance on implementing an {@link
 *     OpenLineageEventHandlerFactory}
 */
class InternalEventHandlerFactory implements OpenLineageEventHandlerFactory {

  private final List<OpenLineageEventHandlerFactory> eventHandlerFactories;
  private final VisitorFactory visitorFactory;

  public InternalEventHandlerFactory() {
    ServiceLoader<OpenLineageEventHandlerFactory> loader =
        ServiceLoader.load(OpenLineageEventHandlerFactory.class);
    eventHandlerFactories =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    loader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
                false)
            .collect(Collectors.toList());

    visitorFactory = VisitorFactoryProvider.getInstance();
  }

  /**
   * Invoke a method on each of the supplied {@link OpenLineageEventHandlerFactory}s and merge the
   * results into a single list.
   *
   * @param factories
   * @param supplier
   * @param <T>
   * @return
   */
  private <T> List<T> generate(
      Collection<OpenLineageEventHandlerFactory> factories,
      Function<OpenLineageEventHandlerFactory, Collection<T>> supplier) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(factories.iterator(), Spliterator.IMMUTABLE), false)
        .flatMap(supplier.andThen(Collection::stream))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<PartialFunction<LogicalPlan, List<InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasets =
        visitorFactory.getInputVisitors(context);

    ImmutableList<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasetVisitors =
        ImmutableList.<PartialFunction<LogicalPlan, List<InputDataset>>>builder()
            .addAll(
                generate(
                    eventHandlerFactories,
                    factory -> factory.createInputDatasetQueryPlanVisitors(context)))
            .addAll(inputDatasets)
            .build();
    context.getInputDatasetQueryPlanVisitors().addAll(inputDatasetVisitors);
    return inputDatasetVisitors;
  }

  @Override
  public Collection<PartialFunction<LogicalPlan, List<OutputDataset>>>
      createOutputDatasetQueryPlanVisitors(OpenLineageContext context) {
    List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasets =
        visitorFactory.getOutputVisitors(context);

    ImmutableList<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasetBuilders =
        ImmutableList.<PartialFunction<LogicalPlan, List<OutputDataset>>>builder()
            .addAll(
                generate(
                    eventHandlerFactories,
                    factory -> factory.createOutputDatasetQueryPlanVisitors(context)))
            .addAll(outputDatasets)
            .build();

    context.getOutputDatasetQueryPlanVisitors().addAll(outputDatasetBuilders);
    return outputDatasetBuilders;
  }

  @Override
  public Collection<PartialFunction<Object, List<InputDataset>>> createInputDatasetBuilder(
      OpenLineageContext context) {
    return generate(eventHandlerFactories, factory -> factory.createInputDatasetBuilder(context));
  }

  @Override
  public Collection<PartialFunction<Object, List<OutputDataset>>> createOutputDatasetBuilder(
      OpenLineageContext context) {
    return generate(eventHandlerFactories, factory -> factory.createOutputDatasetBuilder(context));
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends InputDatasetFacet>>
      createInputDatasetFacetBuilders(OpenLineageContext context) {
    return generate(
        eventHandlerFactories, factory -> factory.createInputDatasetFacetBuilders(context));
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      createOutputDatasetFacetBuilders(OpenLineageContext context) {
    return ImmutableList.<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>builder()
        .addAll(
            generate(
                eventHandlerFactories,
                factory -> factory.createOutputDatasetFacetBuilders(context)))
        .add(new OutputStatisticsOutputDatasetFacetBuilder(context))
        .build();
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends DatasetFacet>> createDatasetFacetBuilders(
      OpenLineageContext context) {
    return generate(eventHandlerFactories, factory -> factory.createDatasetFacetBuilders(context));
  }

  @Override
  public Collection<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return ImmutableList.<CustomFacetBuilder<?, ? extends RunFacet>>builder()
        .addAll(
            generate(eventHandlerFactories, factory -> factory.createRunFacetBuilders((context))))
        .add(
            new ErrorFacetBuilder(),
            new LogicalPlanRunFacetBuilder(context),
            new SparkVersionFacetBuilder(context))
        .build();
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends JobFacet>> createJobFacetBuilders(
      OpenLineageContext context) {
    return generate(eventHandlerFactories, factory -> factory.createJobFacetBuilders(context));
  }
}

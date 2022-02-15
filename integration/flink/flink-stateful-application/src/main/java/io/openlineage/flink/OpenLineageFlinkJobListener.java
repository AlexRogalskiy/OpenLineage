package io.openlineage.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class OpenLineageFlinkJobListener implements JobListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageFlinkJobListener.class);

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        LOGGER.info("-----------onJobSubmitted----------------");
        LOGGER.info("Execution Plan: {}", env.getExecutionPlan());   //No operators defined in streaming topology. Cannot execute
        LOGGER.info("------------------------------------");
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        LOGGER.info("---------onJobExecuted----------");
        LOGGER.info("JobExecutionResult: {}", jobExecutionResult);
        LOGGER.info("------------------------------------");
    }
}

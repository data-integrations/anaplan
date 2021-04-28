package com.google.planningworks.cdap.plugins.sink;

import com.anaplan.client.AnaplanService;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import java.io.OutputStream;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDF Sink plugin for importing data to Anaplan server via the wrapper {@link AnaplanService} of
 * Anaplan API.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(AnaplanSink.PLUGIN_NAME)
@Description("Import data into Anaplan model.")
public class AnaplanSink extends SparkSink<StructuredRecord> {

  public static final String PLUGIN_NAME = "AnaplanSink";
  private static final int THREAD_NUM = 1; // Hardcoded to 1 for working with Anaplan REST API 2.0

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanSink.class);

  private final AnaplanSinkConfig config;

  public AnaplanSink(AnaplanSinkConfig config) {
    this.config = config;
  }

  /**
   * Configures the pipeline and validates the configurations when the pipeline is deployed. If this
   * throws an exception, the pipeline will not be deployed and the user will be shown the error
   * message.
   *
   * @param pipelineConfigurer the collection of the pipeline configuration and context
   * @throws IllegalArgumentException when the configuration validate fails
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer)
      throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  /**
   * Prepares and validates the context before the pipeline is running
   *
   * @param sparkPluginContext context of the spark plugin program in the pipeline
   */
  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) {
    FailureCollector collector = sparkPluginContext.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  /**
   * Sinks the data into Anaplan server via Anaplan REST API.
   *
   * <p>Since Anaplan REST API does not supports the paralleling data upload, the thread number &
   * data partition of the spark program is restricted to 1.
   *
   * @param sparkExecutionPluginContext context of the spark plugin program in the pipeline
   * @param javaRDD Java resilient distributed datasets that contains the data to be sent
   */
  @Override
  public void run(
      SparkExecutionPluginContext sparkExecutionPluginContext, JavaRDD<StructuredRecord> javaRDD) {
    javaRDD.coalesce(THREAD_NUM);

    String header = getSchemaHeader(sparkExecutionPluginContext);

    // There will be always only 1 partition since we coalesce the partition number to 1
    javaRDD.foreachPartition(
        iterator -> {
          AnaplanService.setTaskMetadata(config);
          OutputStream uploadOutputStream = retrieveOutputStreamForServerFile(config);
          uploadOutputStream.write(header.getBytes());
          while (iterator.hasNext()) {
            String data = getDataRow(iterator.next());
            uploadOutputStream.write(data.getBytes());
          }

          uploadOutputStream.close();
          AnaplanService.closeDown();
        });
  }

  private static String getSchemaHeader(SparkExecutionPluginContext sparkExecutionPluginContext) {
    return sparkExecutionPluginContext.getInputSchema().getFields().stream()
        .map(Field::getName)
        .collect(Collectors.joining(/* delimiter = */ ","));
  }

  private static OutputStream retrieveOutputStreamForServerFile(AnaplanSinkConfig config) {
    return AnaplanService.getUploadServerFileOutputStream(
        config.getWorkspaceId(),
        config.getModelId(),
        config.getServerFileName(),
        AnaplanSinkConfig.CHUNK_SIZE);
  }

  private static String getDataRow(StructuredRecord input) {
    String dataRow =
        input.getSchema().getFields().stream()
            .map(Field::getName)
            .map(input::get)
            .map(val -> val == null ? "" : String.valueOf(val))
            .collect(Collectors.joining(/* delimiter = */ ","));
    return dataRow + "\n";
  }
}

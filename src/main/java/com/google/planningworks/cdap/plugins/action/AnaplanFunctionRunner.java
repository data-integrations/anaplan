package com.google.planningworks.cdap.plugins.action;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.FunctionType;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CDF Action plugin for running Anaplan Process & Actions. */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(AnaplanFunctionRunner.PLUGIN_NAME)
@Description("Run Anaplan Process/Action.")
public class AnaplanFunctionRunner extends Action {
  public static final String PLUGIN_NAME = "AnaplanFunctionRunner";

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanFunctionRunner.class);
  private final AnaplanFunctionRunnerConfig config;

  public AnaplanFunctionRunner(AnaplanFunctionRunnerConfig config) {
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
   * Runs the Anaplan function
   *
   * @param context the context of the pipeline run
   * @throws InterruptedException when the error happens during the function run
   */
  @Override
  public void run(ActionContext context) throws InterruptedException {
    LOG.debug(String.format("Running the 'run' method of the %s action.", PLUGIN_NAME));
    AnaplanService.setTaskMetadata(config);
    AnaplanService.runAnaplanFunction(
        config.getWorkspaceId(),
        config.getModelId(),
        config.getFunctionName(),
        FunctionType.valueOf(config.getFunctionType()));
    AnaplanService.closeDown();
  }
}

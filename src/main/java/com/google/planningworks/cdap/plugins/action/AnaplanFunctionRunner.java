/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.planningworks.cdap.plugins.action;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.AnaplanConfig;
import com.anaplan.client.AnaplanService.FunctionType;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import java.net.URI;
import java.net.URISyntaxException;
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
   * @throws ValidationException when the configuration validate fails
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
  }

  /**
   * Runs the Anaplan function
   *
   * @param context the context of the pipeline run
   * @throws IllegalArgumentException when function type value is invalid
   * @throws InterruptedException when the error happens during the function run
   * @throws URISyntaxException when given Anaplan service location or auth location is with an
   *  invalid format.
   * @throws ValidationException when the configuration validate fails
   */
  @Override
  public void run(ActionContext context) throws InterruptedException, URISyntaxException {
    LOG.debug(String.format(
      "Running the 'run' method for the %s function in workspace: %s and model: %s.",
      config.getFunctionName(),
      config.getWorkspaceId(),
      config.getModelId()));

    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    AnaplanConfig anaplanConfig = new AnaplanConfig(
      config.getUsername(),
      config.getPassword(),
      new URI(config.getServiceLocation()),
      new URI(config.getAuthServiceLocation()));
    AnaplanService.setTaskMetadata(anaplanConfig);

    AnaplanService.runAnaplanFunction(
      config.getWorkspaceId(),
      config.getModelId(),
      config.getFunctionName(),
      FunctionType.valueOf(config.getFunctionType()));
    AnaplanService.closeDown();
  }
}

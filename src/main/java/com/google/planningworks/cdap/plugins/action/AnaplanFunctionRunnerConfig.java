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

import com.anaplan.client.AnaplanService.FunctionType;
import com.google.common.base.Strings;
import com.google.planningworks.cdap.plugins.base.AnaplanPluginConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * The config class for {@link AnaplanFunctionRunner} that contains all properties that need to be
 * filled in by the user when building a pipeline.
 */
class AnaplanFunctionRunnerConfig extends AnaplanPluginConfig {
  public static final String NAME_FUNCTION_TYPE = "functionType";
  public static final String NAME_FUNCTION_NAME = "functionName";

  @Name(NAME_FUNCTION_TYPE)
  @Macro
  @Description("Process or action")
  private String functionType;

  @Name(NAME_FUNCTION_NAME)
  @Macro
  @Description("Process or action name")
  private String functionName;

  /**
   * Validates the configure options entered by the user
   *
   * @param collector the shared failure info collector storing the validation failures
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(NAME_FUNCTION_TYPE)) {
      try {
        FunctionType.valueOf(functionType);
      } catch (IllegalArgumentException | NullPointerException e) {
        collector
          .addFailure("Anaplan function type is invalid.", null)
          .withConfigProperty(NAME_FUNCTION_TYPE)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_FUNCTION_NAME) && Strings.isNullOrEmpty(functionName)) {
      collector
        .addFailure("Anaplan function name is required.", null)
        .withConfigProperty(NAME_FUNCTION_NAME);
    }
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getFunctionType() {
    return functionType;
  }
}

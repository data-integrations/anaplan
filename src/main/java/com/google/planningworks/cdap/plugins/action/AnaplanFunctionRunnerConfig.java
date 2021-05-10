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

import static com.anaplan.client.AnaplanService.NAME_AUTH_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_PASSWORD;
import static com.anaplan.client.AnaplanService.NAME_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_USERNAME;
import static com.anaplan.client.AnaplanService.NAME_WORKSPACE_ID;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.FunctionType;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * The config class for {@link AnaplanFunctionRunner} that contains all properties that need to be
 * filled in by the user when building a pipeline.
 */
class AnaplanFunctionRunnerConfig extends PluginConfig {
  public static final String NAME_FUNCTION_TYPE = "functionType";
  public static final String NAME_FUNCTION_NAME = "functionName";

  @Name(NAME_SERVICE_LOCATION)
  @Macro
  @Description("Service location of your Anaplan API")
  private String serviceLocation;

  @Name(NAME_AUTH_SERVICE_LOCATION)
  @Macro
  @Description("Service location of your Anaplan authentication API")
  private String authServiceLocation;

  @Name(NAME_USERNAME)
  @Macro
  @Description("Username")
  private String username;

  @Name(NAME_PASSWORD)
  @Macro
  @Description("Password")
  private String password;

  @Name(NAME_WORKSPACE_ID)
  @Macro
  @Description("WorkspaceId")
  private String workspaceId;

  @Name(NAME_MODEL_ID)
  @Macro
  @Description("ModelId")
  private String modelId;

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
    if (!containsMacro(NAME_SERVICE_LOCATION)) {
      try {
        AnaplanService.validateServiceLocation(serviceLocation);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_SERVICE_LOCATION)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_AUTH_SERVICE_LOCATION)) {
      try {
        AnaplanService.validateAuthServiceLocation(authServiceLocation);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_AUTH_SERVICE_LOCATION)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_USERNAME)) {
      try {
        AnaplanService.validateUsername(username);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_USERNAME)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_PASSWORD)) {
      try {
        AnaplanService.validatePassword(password);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_PASSWORD)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_WORKSPACE_ID)) {
      try {
        AnaplanService.validateWorkspaceId(workspaceId);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_WORKSPACE_ID)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_MODEL_ID)) {
      try {
        AnaplanService.validateModelId(modelId);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_MODEL_ID)
          .withStacktrace(e.getStackTrace());
      }
    }

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

  public String getServiceLocation() { return serviceLocation; }

  public String getAuthServiceLocation() { return authServiceLocation; }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getWorkspaceId() {
    return workspaceId;
  }

  public String getModelId() {
    return modelId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getFunctionType() {
    return functionType;
  }
}

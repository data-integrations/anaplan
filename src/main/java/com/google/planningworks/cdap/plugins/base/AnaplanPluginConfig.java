package com.google.planningworks.cdap.plugins.base;

import com.anaplan.client.AnaplanService;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Baseline class for the AnaplanPlugin configuration.
 */
public class AnaplanPluginConfig extends PluginConfig {

  @Name(AnaplanService.NAME_SERVICE_LOCATION)
  @Macro
  @Description("Service location of your Anaplan API")
  protected String serviceLocation;

  @Name(AnaplanService.NAME_AUTH_SERVICE_LOCATION)
  @Macro
  @Description("Service location of your Anaplan authentication API")
  protected String authServiceLocation;

  @Name(AnaplanService.NAME_USERNAME)
  @Macro
  @Description("Username")
  protected String username;

  @Name(AnaplanService.NAME_PASSWORD)
  @Macro
  @Description("Password")
  protected String password;

  @Name(AnaplanService.NAME_WORKSPACE_ID)
  @Macro
  @Description("WorkspaceId")
  protected String workspaceId;

  @Name(AnaplanService.NAME_MODEL_ID)
  @Macro
  @Description("ModelId")
  protected String modelId;

  /**
   * Validates the configure options entered by the user
   *
   * @param collector the shared failure info collector storing the validation failures
   */
  public void validate(FailureCollector collector) {
    if (!containsMacro(AnaplanService.NAME_SERVICE_LOCATION)) {
      try {
        AnaplanService.validateServiceLocation(serviceLocation);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_SERVICE_LOCATION)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(AnaplanService.NAME_AUTH_SERVICE_LOCATION)) {
      try {
        AnaplanService.validateAuthServiceLocation(authServiceLocation);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_AUTH_SERVICE_LOCATION)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(AnaplanService.NAME_USERNAME)) {
      try {
        AnaplanService.validateUsername(username);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_USERNAME)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(AnaplanService.NAME_PASSWORD)) {
      try {
        AnaplanService.validatePassword(password);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_PASSWORD)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(AnaplanService.NAME_WORKSPACE_ID)) {
      try {
        AnaplanService.validateWorkspaceId(workspaceId);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_WORKSPACE_ID)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(AnaplanService.NAME_MODEL_ID)) {
      try {
        AnaplanService.validateModelId(modelId);
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(AnaplanService.NAME_MODEL_ID)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  public String getServiceLocation() {
    return serviceLocation;
  }

  public String getAuthServiceLocation() {
    return authServiceLocation;
  }

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
}

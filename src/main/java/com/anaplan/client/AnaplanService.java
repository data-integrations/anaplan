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
package com.anaplan.client;

import com.anaplan.client.ex.ActionsNotFoundException;
import com.anaplan.client.ex.ProcessesNotFoundException;
import com.anaplan.client.ex.ServerFilesNotFoundException;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class of the Anaplan service providing the interfaces for a client to connect with
 * Anaplan server.
 */
public class AnaplanService {

  /** Enum class of the Anaplan internal function types. */
  public enum FunctionType {
    PROCESS,
    ACTION
  }

  /** Wrapper for the metadata for the connection with Anaplan server. */
  public static class AnaplanConfig {
    private final URI serviceLocation;
    private final URI authServiceLocation;
    private final String username;
    private final String password;

    public AnaplanConfig(
      String username, String password, URI serviceLocation, URI authServiceLocation) {
      this.username = username;
      this.password = password;
      this.serviceLocation = serviceLocation;
      this.authServiceLocation = authServiceLocation;
    }

    public URI getAuthServiceLocation() {
      return authServiceLocation;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    public URI getServiceLocation() {
      return serviceLocation;
    }
  }

  public static final String NAME_SERVICE_LOCATION = "serviceLocation";
  public static final String NAME_AUTH_SERVICE_LOCATION = "authServiceLocation";
  public static final String NAME_USERNAME = "username";
  public static final String NAME_PASSWORD = "password";
  public static final String NAME_WORKSPACE_ID = "workspaceId";
  public static final String NAME_MODEL_ID = "modelId";
  public static final String NAME_SERVER_FILE_NAME = "serverFileName";

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanService.class);

  /**
   * Sets metadata details for the connection with Anaplan server
   *
   * @param config the config instance carries metadata for the connection with Anaplan server
   */
  public static void setTaskMetadata(AnaplanConfig config) throws URISyntaxException {
    setAPIRoot(config.getServiceLocation(), config.getAuthServiceLocation());
    setCredential(config.getUsername(), config.getPassword());
  }

  private static void setAPIRoot(URI serviceLocation, URI authServiceLocation) {
    Program.setServiceLocation(serviceLocation);
    Program.setAuthServiceLocation(authServiceLocation);
  }

  private static void setCredential(String username, String password) {
    Program.setUsername(username);
    Program.setPassphrase(password);
  }

  /** Closes the connection with Anaplan server */
  public static void closeDown() {
    Program.closeDown();
    LOG.info("Anaplan connection is closed.");
  }

  /**
   * Runs the Anaplan functions defined in the model
   *
   * @param workspaceId the ID of the target workspace
   * @param modelId the ID of the target model
   * @param functionId the ID of the function to be run
   * @param type the type {@link FunctionType} of the function to be run
   * @throws InterruptedException when errors happens during the function run
   * @throws ProcessesNotFoundException when requested process is not defined in the model
   * @throws ActionsNotFoundException when requested action is not defined in the model
   */
  public static void runAnaplanFunction(
    String workspaceId, String modelId, String functionId, FunctionType type)
    throws InterruptedException {
    TaskFactory taskFactory = null;
    switch (type) {
      case PROCESS:
        taskFactory = Program.getProcess(workspaceId, modelId, functionId);
        if (taskFactory == null) {
          throw new ProcessesNotFoundException(
            modelId, new Exception("A process must be specified in model before calling"));
        }
        break;
      case ACTION:
        taskFactory = Program.getAction(workspaceId, modelId, functionId);
        if (taskFactory == null) {
          throw new ActionsNotFoundException(
            modelId, new Exception("An action must be specified in model before calling"));
        }
        break;
    }

    Task task = taskFactory.createTask(new TaskParameters());
    TaskResult lastResult = task.runTask();
    LOG.info("Function run is done successfully:" + lastResult.isSuccessful());
  }

  /**
   * Gets the {@link OutputStream} for uploading a data file
   *
   * @param workspaceId the ID of the target workspace
   * @param modelId the ID of the target model
   * @param fileId the ID of the placeholder in Anaplan model for the file to be loaded
   * @param chunkSize the size of a chunk for uploading
   * @throws ServerFilesNotFoundException when requested serverFile is not found in the model
   * @return the {@link OutputStream} for uploading a data file
   */
  public static OutputStream getUploadServerFileOutputStream(
    String workspaceId, String modelId, String fileId, int chunkSize) {
    ServerFile serverFile = getServerFile(workspaceId, modelId, fileId, true);
    return serverFile.getUploadStream(Program.fetchChunkSize(String.valueOf(chunkSize))); // in MB
  }

  /**
   * Gets the {@link InputStream} for downloading a data file
   *
   * @param workspaceId the ID of the target workspace
   * @param modelId the ID of the target model
   * @param fileId the ID of the file in Anaplan model to be downloaded
   * @throws ServerFilesNotFoundException when requested serverFile is not found in the model
   * @return the {@link InputStream} for downloading a data file
   */
  public static InputStream getDownloadServerFileInputStream(
    String workspaceId, String modelId, String fileId) {
    ServerFile serverFile = getServerFile(workspaceId, modelId, fileId, false);
    return serverFile.getDownloadStream();
  }

  private static ServerFile getServerFile(
    String workspaceId, String modelId, String fileId, boolean create) {
    ServerFile serverFile = Program.getServerFile(workspaceId, modelId, fileId, create);
    if (serverFile == null) {
      throw new ServerFilesNotFoundException(
        modelId,
        new Exception(
          String.format(
            "Server file: %s is not found from workspace: %s and model: %s",
            fileId, workspaceId, modelId)));
    }
    LOG.info("Server file %s is retrieved.", fileId);
    return serverFile;
  }

  /**
   * Validates the username format
   *
   * @throws IllegalArgumentException if the username is not presented
   */
  public static void validateUsername(String username) {
    if (Strings.isNullOrEmpty(username)) {
      throw new IllegalArgumentException("Anaplan username is required.");
    }
  }

  /**
   * Validates the password format
   *
   * @throws IllegalArgumentException if the password is not presented
   */
  public static void validatePassword(String password) {
    if (Strings.isNullOrEmpty(password)) {
      throw new IllegalArgumentException("Anaplan password is required.");
    }
  }

  /**
   * Validates the workspaceId
   *
   * @throws IllegalArgumentException if the workspaceId is not presented
   */
  public static void validateWorkspaceId(String workspaceId) {
    if (Strings.isNullOrEmpty(workspaceId)) {
      throw new IllegalArgumentException("Anaplan workspaceId is required.");
    }
  }

  /**
   * Validates the modelId
   *
   * @throws IllegalArgumentException if the modelId is not presented
   */
  public static void validateModelId(String modelId) {
    if (Strings.isNullOrEmpty(modelId)) {
      throw new IllegalArgumentException("Anaplan modelId is required.");
    }
  }

  /**
   * Validates the service location format
   *
   * @throws IllegalArgumentException if the service location is in an invalid format
   */
  public static void validateServiceLocation(String serviceLocation) {
    try {
      new URI(serviceLocation);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
        String.format("Service location URI is invalid: %s", serviceLocation), e);
    }
  }

  /**
   * Validates the auth service location format
   *
   * @throws IllegalArgumentException if the auth service location is in an invalid format
   */
  public static void validateAuthServiceLocation(String authServiceLocation) {
    try {
      new URI(authServiceLocation);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
        String.format("Authentication service URI is invalid: %s", authServiceLocation), e);
    }
  }
}

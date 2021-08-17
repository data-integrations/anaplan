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
package com.google.planningworks.cdap.plugins.source;

import com.anaplan.client.AnaplanService;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import com.google.planningworks.cdap.plugins.base.AnaplanPluginConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * The config class for {@link AnaplanExport} that contains all properties that need to be filled in
 * by the user when building a pipeline.
 */
public class AnaplanExportConfig extends AnaplanPluginConfig implements FileSourceProperties {

  public static final String AUTO_DETECT = "auto-detect";
  public static final String FILE_CONTENT_TYPE = "text/plain";
  public static final String NAME_PROJECT = "project";
  public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
  public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String NAME_FORMAT = "format";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";

  public static final String NAME_ARCHIVE_NAME = "archiveName";
  public static final String NAME_BUCKET = "bucket";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(
    "This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;

  // GCP service account properties
  @Name(NAME_PROJECT)
  @Description(
    "Google Cloud Project ID, which uniquely identifies a project. "
      + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  protected String project;

  @Name(NAME_SERVICE_ACCOUNT_TYPE)
  @Description(
    "Service account type, file path where the service account is located or the JSON content of"
      + " the service account.")
  @Macro
  @Nullable
  protected String serviceAccountType;

  @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
  @Description(
    "Path on the local file system of the service account key used for authorization. Can be set"
      + " to 'auto-detect' when running on a Dataproc cluster. When running on other clusters,"
      + " the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  protected String serviceFilePath;

  @Name(NAME_SERVICE_ACCOUNT_JSON)
  @Description("Content of the service account file.")
  @Macro
  @Nullable
  protected String serviceAccountJson;

  // Anaplan properties
  @Name(AnaplanService.NAME_SERVER_FILE_NAME)
  @Macro
  @Description("Anaplan Server file name")
  private String serverFileName;

  @Name(NAME_ARCHIVE_NAME)
  @Macro
  @Description("File archive name in GCS bucket")
  private String archiveName;

  // GCS properties
  @Name(NAME_BUCKET)
  @Macro
  @Description("GCS bucket for export file buffer")
  private String bucket;

  @Macro
  @Description(
    "Format of the data to read. Supported formats are 'csv' and 'tsv'.")
  private String format;

  @Macro
  @Nullable
  @Description(
    "Output schema. If a Path Field is set, it must be present in the schema as a string.")
  private String schema;

  /**
   * Validates the configure options entered by the user
   *
   * @param collector the shared failure info collector storing the validation failures
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    IdUtils.validateReferenceName(referenceName, collector);

    // Anaplan properties validation
    if (!containsMacro(AnaplanService.NAME_SERVER_FILE_NAME) && Strings
      .isNullOrEmpty(serverFileName)) {
      collector
        .addFailure("Server file is not presented.", null)
        .withConfigProperty(AnaplanService.NAME_MODEL_ID);
    }

    // GCS properties validation
    if (!containsMacro(NAME_BUCKET) && !containsMacro(NAME_ARCHIVE_NAME)) {
      try {
        GCSPath.from(getPath());
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty("path")
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_FORMAT)) {
      try {
        getFormat();
      } catch (IllegalArgumentException e) {
        collector
          .addFailure(e.getMessage(), null)
          .withConfigProperty(NAME_FORMAT)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }

  /**
   * Gets GCS file path in the format `gs://%s/%s`
   */
  @Override
  public String getPath() {
    return String.format("gs://%s/%s", bucket, archiveName);
  }

  @Override
  public FileFormat getFormat() {
    return FileFormat.from(format, FileFormat::canRead);
  }

  /**
   * Gets the regular {@link Pattern} for the fileRegex filed
   *
   * @throws IllegalArgumentException if fileRegex filed contains an invalid regular expression
   */
  @Nullable
  @Override
  public Pattern getFilePattern() {
    return null;
  }

  @Override
  public long getMaxSplitSize() {
    return 128L * 1024 * 1024;
  }

  /**
   * Gets whether allow empty input. The empty input is blocked for this plugin.
   */
  @Override
  public boolean shouldAllowEmptyInput() {
    return false;
  }

  @Override
  public boolean shouldReadRecursively() {
    return false;
  }

  @Nullable
  @Override
  public String getPathField() {
    return null;
  }

  @Override
  public boolean useFilenameAsPath() {
    return false;
  }

  /**
   * Gets {@link Schema instance} from schema field
   *
   * @throws IllegalArgumentException if the schema cannot be parsed successfully
   */
  @Nullable
  @Override
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean skipHeader() {
    return true;
  }

  /**
   * Gets project ID from filed/macro/context
   *
   * @throws IllegalArgumentException if the project ID cannot be retrieved successfully
   */
  public String getProject() {
    String projectId = tryGetProject();
    if (projectId == null) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud project id from the environment. Please specify a project"
          + " id.");
    }
    return projectId;
  }

  @Nullable
  private String tryGetProject() {
    if (containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)) {
      return null;
    }
    String projectId = project;
    if (Strings.isNullOrEmpty(project) || AUTO_DETECT.equals(project)) {
      projectId = ServiceOptions.getDefaultProjectId();
    }
    return projectId;
  }

  /**
   * Gets Service Account Type, defaults to filePath.
   */
  @Nullable
  public String getServiceAccountType() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_TYPE)) {
      return null;
    }
    return Strings.isNullOrEmpty(serviceAccountType)
      ? SERVICE_ACCOUNT_FILE_PATH
      : serviceAccountType;
  }

  @Nullable
  public String getServiceAccount() {
    Boolean serviceAccountJson = isServiceAccountJson();
    if (serviceAccountJson == null) {
      return null;
    }
    return serviceAccountJson ? getServiceAccountJson() : getServiceAccountFilePath();
  }

  @Nullable
  private Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType)
      ? null
      : serviceAccountType.equals(SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  private String getServiceAccountJson() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  @Nullable
  private String getServiceAccountFilePath() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH)
      || serviceFilePath == null
      || serviceFilePath.isEmpty()
      || AUTO_DETECT.equals(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }

  public String getServerFileName() {
    return serverFileName;
  }

  public String getArchiveName() {
    return archiveName;
  }

  public String getBucket() {
    return bucket;
  }
}

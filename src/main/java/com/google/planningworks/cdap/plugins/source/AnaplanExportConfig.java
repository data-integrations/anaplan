package com.google.planningworks.cdap.plugins.source;

import static com.anaplan.client.AnaplanService.NAME_AUTH_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_PASSWORD;
import static com.anaplan.client.AnaplanService.NAME_SERVER_FILE_NAME;
import static com.anaplan.client.AnaplanService.NAME_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_USERNAME;
import static com.anaplan.client.AnaplanService.NAME_WORKSPACE_ID;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.AnaplanConfig;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.charset.fixedlength.FixedLengthCharset;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * The config class for {@link AnaplanExport} that contains all properties that need to be filled in
 * by the user when building a pipeline.
 */
public class AnaplanExportConfig extends PluginConfig
    implements FileSourceProperties, AnaplanConfig {

  public static final String AUTO_DETECT = "auto-detect";
  public static final String FILE_CONTENT_TYPE = "text/plain";
  public static final String NAME_PROJECT = "project";
  public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
  public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String NAME_PATH = "path";
  public static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
  public static final String NAME_FILE_REGEX = "fileRegex";
  public static final String NAME_FORMAT = "format";
  public static final String NAME_DELIMITER = "delimiter";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";

  public static final String NAME_ARCHIVE_NAME = "archiveName";
  public static final String NAME_BUCKET = "bucket";

  private static final String DEFAULT_ENCRYPTED_METADATA_SUFFIX = ".metadata";

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE =
      new TypeToken<Map<String, String>>() {}.getType();

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

  @Name(NAME_SERVER_FILE_NAME)
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
  @Nullable
  @Description("Map of properties to set on the InputFormat.")
  private String fileSystemProperties;

  @Macro
  @Nullable
  @Description(
      "Maximum size of each partition used to read data. Smaller partitions will increase the level"
          + " of parallelism, but will require more resources and overhead.")
  private Long maxSplitSize;

  @Macro
  @Nullable
  @Description("Minimum size of each partition used to read data. ")
  private Long minSplitSize;

  @Macro
  @Nullable
  @Description(
      "Output field to place the path of the file that the record was read from. "
          + "If not specified, the file path will not be included in output records. "
          + "If specified, the field must exist in the output schema as a string.")
  private String pathField;

  @Macro
  @Description(
      "Format of the data to read. Supported formats are 'avro', 'blob', 'csv', 'delimited',"
          + " 'json', 'parquet', 'text', and 'tsv'.")
  private String format;

  @Macro
  @Nullable
  @Description(
      "Output schema. If a Path Field is set, it must be present in the schema as a string.")
  private String schema;

  @Macro
  @Nullable
  @Description(
      "Whether to only use the filename instead of the URI of the file path when a path field is"
          + " given. The default value is false.")
  private Boolean filenameOnly;

  @Macro
  @Nullable
  @Description(
      "Regular expression that file paths must match in order to be included in the input. The full"
          + " file path is compared, not just the file name.If no value is given, no file filtering"
          + " will be done. See"
          + " https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more"
          + " information about the regular expression syntax.")
  private String fileRegex;

  @Macro
  @Nullable
  @Description(
      "Whether to recursively read directories within the input directory. The default is false.")
  private Boolean recursive;

  @Macro
  @Nullable
  @Description(
      "The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the"
          + " format is anything other than 'delimited'.")
  private String delimiter;

  @Macro
  @Nullable
  @Description(
      "Whether to skip the first line of each file. Supported formats are 'text', 'csv', 'tsv', "
          + "'delimited'. Default value is false.")
  private Boolean skipHeader;

  @Macro
  @Nullable
  @Description("File encoding for the source files. The default encoding is 'UTF-8'")
  private String fileEncoding;

  // This is a hidden property that only exists for wrangler's parse-as-csv that uses the header as
  // the schema. When this is true and the format is text, the header will be the first record
  // returned by every record reader
  @Nullable private Boolean copyHeader;

  @Macro
  @Nullable
  @Description(
      "Whether the data file is encrypted. If it is set to 'true', a associated metadata file needs"
          + " to be provided for each data file. Please refer to the Documentation for the details"
          + " of the metadata file content.")
  private Boolean encrypted;

  @Macro
  @Nullable
  @Description(
      "The file name suffix for the metadata file of the encrypted data file. "
          + "The default is '"
          + DEFAULT_ENCRYPTED_METADATA_SUFFIX
          + "'.")
  private String encryptedMetadataSuffix;

  public AnaplanExportConfig() {
    this.maxSplitSize = 128L * 1024 * 1024;
    this.recursive = false;
    this.filenameOnly = false;
    this.copyHeader = false;
  }

  /**
   * Validates the configure options entered by the user
   *
   * @param collector the shared failure info collector storing the validation failures
   */
  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    // Anaplan properties validation
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

    if (!containsMacro(NAME_SERVER_FILE_NAME) && Strings.isNullOrEmpty(serverFileName)) {
      collector
          .addFailure("Server file is not presented.", null)
          .withConfigProperty(NAME_MODEL_ID);
    }

    // GCS properties validation
    if (!containsMacro(NAME_PATH)) {
      try {
        GCSPath.from(getPath());
      } catch (IllegalArgumentException e) {
        collector
            .addFailure(e.getMessage(), null)
            .withConfigProperty(NAME_PATH)
            .withStacktrace(e.getStackTrace());
      }
    }
    if (!containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
      try {
        getFileSystemProperties();
      } catch (Exception e) {
        collector
            .addFailure("File system properties must be a valid json.", null)
            .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES)
            .withStacktrace(e.getStackTrace());
      }
    }
    if (!containsMacro(NAME_FILE_REGEX)) {
      try {
        getFilePattern();
      } catch (IllegalArgumentException e) {
        collector
            .addFailure(e.getMessage(), null)
            .withConfigProperty(NAME_FILE_REGEX)
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

    if (fileEncoding != null
        && !fileEncoding.equals(AbstractFileSourceConfig.DEFAULT_FILE_ENCODING)
        && !FixedLengthCharset.isValidEncoding(fileEncoding)) {
      collector.addFailure(
          "Specified file encoding is not valid.", "Use one of the supported file encodings.");
    }

    if (!containsMacro(NAME_FILE_REGEX)) {
      try {
        getFilePattern();
      } catch (IllegalArgumentException e) {
        collector
            .addFailure(e.getMessage(), null)
            .withConfigProperty(NAME_FILE_REGEX)
            .withStacktrace(e.getStackTrace());
      }
    }
  }

  @Override
  public String getReferenceName() {
    return referenceName;
  }

  /** Gets GCS file path in the format `gs://%s/%s` */
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
    try {
      return fileRegex == null ? null : Pattern.compile(fileRegex);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid file regular expression." + e.getMessage(), e);
    }
  }

  @Override
  public long getMaxSplitSize() {
    return maxSplitSize;
  }

  @Nullable
  public Long getMinSplitSize() {
    return minSplitSize;
  }

  /** Gets whether allow empty input. The empty input is blocked for this plugin. */
  @Override
  public boolean shouldAllowEmptyInput() {
    return false;
  }

  @Override
  public boolean shouldReadRecursively() {
    return recursive;
  }

  @Nullable
  @Override
  public String getPathField() {
    return pathField;
  }

  @Override
  public boolean useFilenameAsPath() {
    return filenameOnly;
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

  public boolean isCopyHeader() {
    return copyHeader != null && copyHeader;
  }

  @Override
  public boolean skipHeader() {
    return skipHeader == null ? false : skipHeader;
  }

  @Nullable
  public String getFileEncoding() {
    return fileEncoding;
  }

  public boolean isEncrypted() {
    return encrypted != null && encrypted;
  }

  Map<String, String> getFileSystemProperties() {
    if (fileSystemProperties == null) {
      return Collections.emptyMap();
    }
    return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
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

  /** Gets Service Account Type, defaults to filePath. */
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

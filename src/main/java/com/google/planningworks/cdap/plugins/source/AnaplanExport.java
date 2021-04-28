package com.google.planningworks.cdap.plugins.source;

import com.anaplan.client.AnaplanService;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDF Source plugin for sourcing data from Anaplan server via the wrapper {@link AnaplanService} of
 * Anaplan API.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(AnaplanExport.PLUGIN_NAME)
@Description("Export data from Anaplan using GCS as buffer.")
public class AnaplanExport extends AbstractFileSource<AnaplanExportConfig> {
  public static final String PLUGIN_NAME = "AnaplanExport";
  public static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  public static final String CLOUD_JSON_KEYFILE_SUFFIX = "auth.service.account.json.keyfile";
  public static final String CLOUD_JSON_KEYFILE_PREFIX = "google.cloud";
  public static final String CLOUD_ACCOUNT_EMAIL_SUFFIX = "auth.service.account.email";
  public static final String CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX =
      "auth.service.account.private.key.id";
  public static final String CLOUD_ACCOUNT_KEY_SUFFIX = "auth.service.account.private.key";
  public static final String CLOUD_ACCOUNT_JSON_SUFFIX = "auth.service.account.json";
  public static final String SERVICE_ACCOUNT_TYPE = "cdap.gcs.auth.service.account.type";
  public static final String SERVICE_ACCOUNT_TYPE_FILE_PATH = "filePath";
  public static final String PRIVATE_KEY_WRAP =
      "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n";

  private final AnaplanExportConfig config;

  private static final Logger LOG = LoggerFactory.getLogger(AnaplanExport.class);

  public AnaplanExport(AnaplanExportConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Configures the pipeline and validates the configurations when the pipeline is deployed. If this
   * throws an exception, the pipeline will not be deployed and the user will be shown the error
   * message.
   *
   * @param pipelineConfigurer the collection of the pipeline configuration and context
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  /**
   * Prepares and validates the context before the pipeline is running. The data file from Anaplan
   * server is downloaded into the given GCS bucket vis this process.
   *
   * @param context context of the pipeline run in this job
   */
  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    // Load data from Anaplan to GCS bucket
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(config.getProject());
    Storage storage = builder.build().getService();
    if (storage.get(config.getBucket()) == null) {
      storage.create(BucketInfo.of(config.getBucket()));
    }

    BlobId blobId = BlobId.of(config.getBucket(), config.getArchiveName());
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId).setContentType(AnaplanExportConfig.FILE_CONTENT_TYPE).build();

    AnaplanService.setTaskMetadata(config);
    InputStream inputStream =
        AnaplanService.getDownloadServerFileInputStream(
            config.getWorkspaceId(), config.getModelId(), config.getServerFileName());

    try (WriteChannel writer = storage.writer(blobInfo)) {
      ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
    } catch (IOException ex) {
      throw ex;
    }

    inputStream.close();
    AnaplanService.closeDown();

    LOG.info(
        "Anaplan data is achieved in GCS bucket %s as %s.",
        config.getBucket(), config.getArchiveName());

    // Continue sourcing data from GCS bucket
    super.prepareRun(context);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties =
        getFileSystemProperties(
            config, config.getPath(), new HashMap<>(config.getFileSystemProperties()));
    if (config.isCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, Boolean.TRUE.toString());
    }
    if (config.getFileEncoding() != null
        && !config
        .getFileEncoding()
        .equalsIgnoreCase(AbstractFileSourceConfig.DEFAULT_FILE_ENCODING)) {
      properties.put(PathTrackingInputFormat.SOURCE_FILE_ENCODING, config.getFileEncoding());
    }
    if (config.getMinSplitSize() != null) {
      properties.put(
          "mapreduce.input.fileinputformat.split.minsize",
          String.valueOf(config.getMinSplitSize()));
    }

    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead(
        "Read",
        String.format(
            "Read%sfrom Google Cloud Storage.", config.isEncrypted() ? " and decrypt " : " "),
        outputFields);
  }

  @Override
  protected boolean shouldGetSchema() {
    return !config.containsMacro(AnaplanExportConfig.NAME_PROJECT)
        && !config.containsMacro(AnaplanExportConfig.NAME_PATH)
        && !config.containsMacro(AnaplanExportConfig.NAME_FORMAT)
        && !config.containsMacro(AnaplanExportConfig.NAME_DELIMITER)
        && !config.containsMacro(AnaplanExportConfig.NAME_FILE_SYSTEM_PROPERTIES)
        && !config.containsMacro(AnaplanExportConfig.NAME_SERVICE_ACCOUNT_FILE_PATH)
        && !config.containsMacro(AnaplanExportConfig.NAME_SERVICE_ACCOUNT_JSON);
  }

  private static Map<String, String> getFileSystemProperties(
      AnaplanExportConfig config, String path, Map<String, String> properties) {
    try {
      properties.putAll(
          generateAuthProperties(
              config.getServiceAccount(),
              config.getServiceAccountType(),
              CLOUD_JSON_KEYFILE_PREFIX));
    } catch (Exception ignored) {
      // Exception ignored
    }
    properties.put("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    properties.put("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
    String projectId = config.getProject();
    properties.put(FS_GS_PROJECT_ID, projectId);
    properties.put("fs.gs.system.bucket", GCSPath.from(path).getBucket());
    properties.put("fs.gs.path.encoding", "uri-path");
    properties.put("fs.gs.working.dir", GCSPath.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    return properties;
  }

  /**
   * @param serviceAccount file path or Json content
   * @param serviceAccountType type of service account can be filePath or json
   * @param keyPrefix list of prefixes for which additional properties will be set. <br>
   *     for account type filePath:
   *     <ul>
   *       <li>prefix + auth.service.account.json
   *     </ul>
   *     for account type json:
   *     <ul>
   *       <li>prefix + auth.service.account.email
   *       <li>prefix + auth.service.account.private.key.id
   *       <li>prefix + auth.service.account.private.key
   *     </ul>
   *
   * @return {@link Map<String,String>} properties genereated based on input params
   * @throws IOException
   */
  private static Map<String, String> generateAuthProperties(
      String serviceAccount, String serviceAccountType, String... keyPrefix) throws IOException {
    Map<String, String> properties = new HashMap<>();
    if (serviceAccountType == null) {
      return properties;
    }
    String privateKeyData = null;
    properties.put(SERVICE_ACCOUNT_TYPE, serviceAccountType);

    boolean isServiceAccountFilePath = SERVICE_ACCOUNT_TYPE_FILE_PATH.equals(serviceAccountType);

    for (String prefix : keyPrefix) {
      if (isServiceAccountFilePath) {
        if (serviceAccount != null) {
          properties.put(String.format("%s.%s", prefix, CLOUD_JSON_KEYFILE_SUFFIX), serviceAccount);
        }
        continue;
      }
      ServiceAccountCredentials credentials = loadServiceAccountCredentials(serviceAccount, false);

      properties.put(
          String.format("%s.%s", prefix, CLOUD_ACCOUNT_EMAIL_SUFFIX), credentials.getClientEmail());
      properties.put(
          String.format("%s.%s", prefix, CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX),
          credentials.getPrivateKeyId());
      if (privateKeyData == null) {
        privateKeyData = extractPrivateKey(credentials);
      }
      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_KEY_SUFFIX), privateKeyData);
      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_JSON_SUFFIX), serviceAccount);
    }
    return properties;
  }

  private static ServiceAccountCredentials loadServiceAccountCredentials(
      String content, boolean isServiceAccountFilePath) throws IOException {
    if (isServiceAccountFilePath) {
      return loadServiceAccountCredentials(content);
    }
    InputStream jsonInputStream = new ByteArrayInputStream(content.getBytes());
    return ServiceAccountCredentials.fromStream(jsonInputStream);
  }

  private static ServiceAccountCredentials loadServiceAccountCredentials(String path)
      throws IOException {
    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }

  private static String extractPrivateKey(ServiceAccountCredentials credentials) {
    return String.format(
        PRIVATE_KEY_WRAP,
        Base64.getEncoder().encodeToString(credentials.getPrivateKey().getEncoded()));
  }
}

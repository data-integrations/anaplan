package com.google.planningworks.cdap.plugins.sink;

import com.anaplan.client.AnaplanService;
import com.anaplan.client.AnaplanService.AnaplanConfig;
import com.anaplan.client.ex.ServerFilesNotFoundException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema.Field;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Spark function to be run by the JavaRDD for uploading data to Anaplan server.
 */
public class SinkToAnaplanProcess implements VoidFunction<Iterator<StructuredRecord>> {

  private final String workspaceId;
  private final String modelId;
  private final String serverFileName;
  private final int chunkSize;
  private final String header;
  private final String username;
  private final String password;
  private final URI serviceLocation;
  private final URI authServiceLocation;

  public SinkToAnaplanProcess(String header, String workspaceId,
    String modelId, String serverFileName, int chunkSize, String username, String password,
    URI serviceLocation, URI authServiceLocation) {
    this.workspaceId = workspaceId;
    this.modelId = modelId;
    this.serverFileName = serverFileName;
    this.chunkSize = chunkSize;
    this.header = header;
    this.username = username;
    this.password = password;
    this.serviceLocation = serviceLocation;
    this.authServiceLocation = authServiceLocation;
  }

  /**
   * Sinks the data into Anaplan server via Anaplan REST API.
   *
   * <p>Since Anaplan REST API does not supports the paralleling data upload, the thread number &
   * data partition of the spark program is restricted to 1 and this function will be called only
   * once for uploading a data file.
   *
   * @throws IOException – when an I/O error occurs during uploading the file
   * @throws ServerFilesNotFoundException – when requested serverFile is not found in the model
   */
  @Override
  public void call(Iterator<StructuredRecord> iterator) throws Exception {
    AnaplanConfig anaplanConfig = new AnaplanConfig(
      username,
      password,
      serviceLocation,
      authServiceLocation);
    AnaplanService.setTaskMetadata(anaplanConfig);
    OutputStream uploadOutputStream = retrieveOutputStreamForServerFile(
      workspaceId,
      modelId,
      serverFileName,
      chunkSize);

    uploadOutputStream.write(header.getBytes());

    while (iterator.hasNext()) {
      String data = getDataRow(iterator.next());
      uploadOutputStream.write(data.getBytes());
    }

    uploadOutputStream.close();
    AnaplanService.closeDown();
  }

  private static OutputStream retrieveOutputStreamForServerFile(String workspaceId, String modelId,
    String serverFileName, int chunkSize) {
    return AnaplanService.getUploadServerFileOutputStream(
      workspaceId,
      modelId,
      serverFileName,
      chunkSize);
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

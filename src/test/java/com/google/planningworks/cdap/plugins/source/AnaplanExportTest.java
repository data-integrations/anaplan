package com.google.planningworks.cdap.plugins.source;

import static com.anaplan.client.AnaplanService.NAME_AUTH_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_PASSWORD;
import static com.anaplan.client.AnaplanService.NAME_SERVER_FILE_NAME;
import static com.anaplan.client.AnaplanService.NAME_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_USERNAME;
import static com.anaplan.client.AnaplanService.NAME_WORKSPACE_ID;
import static com.google.planningworks.cdap.plugins.source.AnaplanExportConfig.NAME_ARCHIVE_NAME;
import static com.google.planningworks.cdap.plugins.source.AnaplanExportConfig.NAME_BUCKET;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.FieldSetter;

@RunWith(JUnit4.class)
public class AnaplanExportTest {

  AnaplanExportConfig config;
  MockFailureCollector failureCollector;

  @Before
  public void setUp() throws Exception {
    failureCollector = new MockFailureCollector("AnaplanSource");
    config = getConfig();
  }

  @Test
  public void testParameterValidationPass() {
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyServerFileName() throws Exception {
    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidServiceLocation() throws Exception {
    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(NAME_SERVICE_LOCATION), "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidAuthServiceLocation() throws Exception {
    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(NAME_AUTH_SERVICE_LOCATION),
        "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyUserName() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_USERNAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyPassword() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_PASSWORD), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyWorkspaceID() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_WORKSPACE_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyModelID() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_MODEL_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyReferenceName() throws Exception {
    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(Constants.Reference.REFERENCE_NAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidFilePath() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_BUCKET), "sl:=+");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidFileFormat() throws Exception {
    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField("format"), "abcdefg");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  private AnaplanExportConfig getConfig() throws Exception {
    AnaplanExportConfig config = new AnaplanExportConfig();

    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(Constants.Reference.REFERENCE_NAME),
        "test-source-plugin");

    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(NAME_SERVICE_LOCATION),
        "https://google.anaplan.com");

    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(NAME_AUTH_SERVICE_LOCATION),
        "https://google.anaplan.com");

    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(NAME_USERNAME), "username@gmail.com");

    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_PASSWORD), "pass");

    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(NAME_WORKSPACE_ID),
        "8a81b01068d4b6820169da807d121dtt");

    FieldSetter.setField(
        config,
        AnaplanExportConfig.class.getDeclaredField(NAME_MODEL_ID),
        "6387EC16A2104DECA9F56AB3C9BD54PP");

    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "test.csv");

    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField(NAME_BUCKET), "bucket");

    FieldSetter.setField(
        config, AnaplanExportConfig.class.getDeclaredField(NAME_ARCHIVE_NAME), "archive.csv");

    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField("format"), "csv");

    return config;
  }
}

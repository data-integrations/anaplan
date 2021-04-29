package com.google.planningworks.cdap.plugins.sink;

import static com.anaplan.client.AnaplanService.NAME_AUTH_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_PASSWORD;
import static com.anaplan.client.AnaplanService.NAME_SERVER_FILE_NAME;
import static com.anaplan.client.AnaplanService.NAME_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_USERNAME;
import static com.anaplan.client.AnaplanService.NAME_WORKSPACE_ID;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.FieldSetter;

@RunWith(JUnit4.class)
public class AnaplanSinkTest {

  AnaplanSinkConfig config;
  MockFailureCollector failureCollector;

  @Before
  public void setUp() throws Exception {
    failureCollector = new MockFailureCollector("AnaplanSinkTest");
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
        config, AnaplanSinkConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidServiceLocation() throws Exception {
    FieldSetter.setField(
        config, AnaplanSinkConfig.class.getDeclaredField(NAME_SERVICE_LOCATION), "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidAuthServiceLocation() throws Exception {
    FieldSetter.setField(
        config,
        AnaplanSinkConfig.class.getDeclaredField(NAME_AUTH_SERVICE_LOCATION),
        "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyUserName() throws Exception {
    FieldSetter.setField(config, AnaplanSinkConfig.class.getDeclaredField(NAME_USERNAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyPassword() throws Exception {
    FieldSetter.setField(config, AnaplanSinkConfig.class.getDeclaredField(NAME_PASSWORD), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyWorkspaceID() throws Exception {
    FieldSetter.setField(config, AnaplanSinkConfig.class.getDeclaredField(NAME_WORKSPACE_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyModelID() throws Exception {
    FieldSetter.setField(config, AnaplanSinkConfig.class.getDeclaredField(NAME_MODEL_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  private AnaplanSinkConfig getConfig() throws Exception {
    AnaplanSinkConfig config = new AnaplanSinkConfig();

    FieldSetter.setField(
        config,
        AnaplanSinkConfig.class.getDeclaredField(NAME_SERVICE_LOCATION),
        "https://mock.anaplan.com");

    FieldSetter.setField(
        config,
        AnaplanSinkConfig.class.getDeclaredField(NAME_AUTH_SERVICE_LOCATION),
        "https://mock.anaplan.com");

    FieldSetter.setField(
        config, AnaplanSinkConfig.class.getDeclaredField(NAME_USERNAME), "username@gmail.com");

    FieldSetter.setField(config, AnaplanSinkConfig.class.getDeclaredField(NAME_PASSWORD), "pass");

    FieldSetter.setField(
        config,
        AnaplanSinkConfig.class.getDeclaredField(NAME_WORKSPACE_ID),
        "8a81b01068d4b6820169da807d121dtt");

    FieldSetter.setField(
        config,
        AnaplanSinkConfig.class.getDeclaredField(NAME_MODEL_ID),
        "6387EC16A2104DECA9F56AB3C9BD54PP");

    FieldSetter.setField(
        config, AnaplanSinkConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "test.csv");

    return config;
  }
}

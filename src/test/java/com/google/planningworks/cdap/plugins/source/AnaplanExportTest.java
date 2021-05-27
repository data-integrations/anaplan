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
import com.google.planningworks.cdap.plugins.base.AnaplanPluginConfig;
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
      config, AnaplanExportConfig.class.getDeclaredField(AnaplanService.NAME_SERVER_FILE_NAME), "");
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
    FieldSetter
      .setField(config, AnaplanExportConfig.class.getDeclaredField(AnaplanExportConfig.NAME_BUCKET),
        "sl: =+");
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
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_AUTH_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_USERNAME),
      "username@gmail.com");

    FieldSetter
      .setField(config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_PASSWORD),
        "pass");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_WORKSPACE_ID),
      "8a81b01068d4b6820169da807d121dtt");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_MODEL_ID),
      "6387EC16A2104DECA9F56AB3C9BD54PP");

    FieldSetter.setField(
      config, AnaplanExportConfig.class.getDeclaredField(AnaplanService.NAME_SERVER_FILE_NAME),
      "test.csv");

    FieldSetter
      .setField(config, AnaplanExportConfig.class.getDeclaredField(AnaplanExportConfig.NAME_BUCKET),
        "bucket");

    FieldSetter.setField(
      config, AnaplanExportConfig.class.getDeclaredField(AnaplanExportConfig.NAME_ARCHIVE_NAME),
      "archive.csv");

    FieldSetter.setField(config, AnaplanExportConfig.class.getDeclaredField("format"), "csv");

    return config;
  }
}

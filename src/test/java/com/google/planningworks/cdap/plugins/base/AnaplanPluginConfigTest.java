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
package com.google.planningworks.cdap.plugins.base;

import com.anaplan.client.AnaplanService;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.FieldSetter;

@RunWith(JUnit4.class)
public class AnaplanPluginConfigTest {

  AnaplanPluginConfig config;
  MockFailureCollector failureCollector;

  @Before
  public void setUp() throws Exception {
    failureCollector = new MockFailureCollector("AnaplanFunctionRunner");
    config = getConfig();
  }

  @Test
  public void testParameterValidationPass() {
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidServiceLocation() throws Exception {
    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_SERVICE_LOCATION),
      "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testInvalidAuthServiceLocation() throws Exception {
    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_AUTH_SERVICE_LOCATION),
      "invalid uri");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyUserName() throws Exception {
    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_USERNAME), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyPassword() throws Exception {
    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_PASSWORD), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyWorkspaceID() throws Exception {
    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_WORKSPACE_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyModelID() throws Exception {
    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_MODEL_ID), "");
    config.validate(failureCollector);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  private AnaplanPluginConfig getConfig() throws Exception {
    AnaplanPluginConfig config = new AnaplanPluginConfig();

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_AUTH_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_USERNAME),
      "username@gmail.com");

    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_PASSWORD), "pass");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_WORKSPACE_ID),
      "8a81b01068d4b6820169da807d121dtt");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(AnaplanService.NAME_MODEL_ID),
      "6387EC16A2104DECA9F56AB3C9BD54PP");

    return config;
  }
}

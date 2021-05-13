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
package com.google.planningworks.cdap.plugins.sink;

import static com.anaplan.client.AnaplanService.NAME_AUTH_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_PASSWORD;
import static com.anaplan.client.AnaplanService.NAME_SERVER_FILE_NAME;
import static com.anaplan.client.AnaplanService.NAME_SERVICE_LOCATION;
import static com.anaplan.client.AnaplanService.NAME_USERNAME;
import static com.anaplan.client.AnaplanService.NAME_WORKSPACE_ID;

import com.google.planningworks.cdap.plugins.base.AnaplanPluginConfig;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
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
  Schema schema;
  MockFailureCollector failureCollector;

  @Before
  public void setUp() throws Exception {
    failureCollector = new MockFailureCollector("AnaplanSinkTest");
    config = getConfig();
    schema = getSupportedSchema();
  }

  @Test
  public void testParameterValidationPass() {
    config.validate(failureCollector, schema);
    Assert.assertEquals(/*expected =*/ 0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testUnsupportedSchema() {
    schema = getUnsupportedSchema();
    config.validate(failureCollector, schema);
    Assert.assertEquals(/*expected =*/ 5, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testEmptyServerFileName() throws Exception {
    FieldSetter.setField(
      config, AnaplanSinkConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "");
    config.validate(failureCollector, schema);
    Assert.assertEquals(/*expected =*/ 1, failureCollector.getValidationFailures().size());
  }

  private Schema getSupportedSchema() {
    return Schema.recordOf("record",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("date", Schema.of(LogicalType.DATE)),
      Schema.Field.of("decimal", Schema.decimalOf(20)));
  }

  private Schema getUnsupportedSchema() {
    return Schema.recordOf("record",
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("timestamp millis", Schema.of(LogicalType.TIMESTAMP_MILLIS)),
      Schema.Field.of("timestamp micros", Schema.of(LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("time millis", Schema.of(LogicalType.TIME_MILLIS)),
      Schema.Field.of("time micros", Schema.of(LogicalType.TIME_MICROS)));
  }

  private AnaplanSinkConfig getConfig() throws Exception {
    AnaplanSinkConfig config = new AnaplanSinkConfig();

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(NAME_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(NAME_AUTH_SERVICE_LOCATION),
      "https://mock.anaplan.com");

    FieldSetter.setField(
      config, AnaplanPluginConfig.class.getDeclaredField(NAME_USERNAME), "username@gmail.com");

    FieldSetter.setField(config, AnaplanPluginConfig.class.getDeclaredField(NAME_PASSWORD), "pass");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(NAME_WORKSPACE_ID),
      "8a81b01068d4b6820169da807d121dtt");

    FieldSetter.setField(
      config,
      AnaplanPluginConfig.class.getDeclaredField(NAME_MODEL_ID),
      "6387EC16A2104DECA9F56AB3C9BD54PP");

    FieldSetter.setField(
      config, AnaplanSinkConfig.class.getDeclaredField(NAME_SERVER_FILE_NAME), "test.csv");

    return config;
  }
}

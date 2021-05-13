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

import static com.anaplan.client.AnaplanService.NAME_MODEL_ID;
import static com.anaplan.client.AnaplanService.NAME_SERVER_FILE_NAME;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.planningworks.cdap.plugins.base.AnaplanPluginConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The config class for {@link AnaplanSink} that contains all properties that need to be filled in
 * by the user when building a pipeline.
 */
class AnaplanSinkConfig extends AnaplanPluginConfig {

  public static final int CHUNK_SIZE = 10;
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.FLOAT,
      Schema.Type.DOUBLE,
      Schema.Type.BOOLEAN);
  public static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES =
    ImmutableSet.of(Schema.LogicalType.DATE, Schema.LogicalType.DECIMAL);

  @Name(NAME_SERVER_FILE_NAME)
  @Macro
  @Description("Anaplan Server file name")
  private String serverFileName;

  /**
   * Validates the configure options entered by the user
   *
   * @param collector the shared failure info collector storing the validation failures
   */
  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    if (!containsMacro(NAME_SERVER_FILE_NAME) && Strings.isNullOrEmpty(serverFileName)) {
      collector
        .addFailure("Server file is not presented.", null)
        .withConfigProperty(NAME_MODEL_ID);
    }

    validateSchemaSupportedByAnaplan(schema, collector);
  }

  private void validateSchemaSupportedByAnaplan(Schema schema, FailureCollector collector) {
    if (schema == null || schema.getFields() == null) {
      return;
    }

    for (Schema.Field field : schema.getFields()) {
      validateFieldSupportedByAnaplan(field, collector);
    }
  }

  private static void validateFieldSupportedByAnaplan(Schema.Field field,
    FailureCollector collector) {
    Schema fieldSchema = getNonNullableSchema(field.getSchema());
    Schema.Type type = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    // validate logical types
    if (logicalType != null) {
      if (!SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        collector.addFailure(generateUnsupportedTypeErrorMessage(field, fieldSchema),
          generateUnsupportedTypeCorrectiveAction());
      }
    } else { // validate types
      if (!SUPPORTED_TYPES.contains(type)) {
        collector.addFailure(generateUnsupportedTypeErrorMessage(field, fieldSchema),
          generateUnsupportedTypeCorrectiveAction());
        return;
      }
    }
  }

  private static Schema getNonNullableSchema(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable() : schema;
  }

  private static String generateUnsupportedTypeErrorMessage(Schema.Field field,
    Schema fieldSchema) {
    return String.format("Field '%s' is of unsupported type '%s'.", field.getName(),
      fieldSchema.getDisplayName());
  }

  private static String generateUnsupportedTypeCorrectiveAction() {
    return String.format("Supported types are: %s, %s.",
      SUPPORTED_TYPES.stream().map(t -> t.name().toLowerCase())
        .collect(Collectors.joining(", ")),
      SUPPORTED_LOGICAL_TYPES.stream().map(t -> t.name().toLowerCase())
        .collect(Collectors.joining(", ")));
  }


  public String getServerFileName() {
    return serverFileName;
  }
}

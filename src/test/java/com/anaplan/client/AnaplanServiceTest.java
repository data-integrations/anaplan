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
package com.anaplan.client;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AnaplanServiceTest {

  @Test
  public void validateUsernamePass() {
    AnaplanService.validateUsername("validuser@gmail.com");
  }

  @Test
  public void validateUsernameThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(IllegalArgumentException.class, () -> AnaplanService.validateUsername(""));
    assertTrue(thrown.getMessage().contains("Anaplan username is required."));
  }

  @Test
  public void validatePasswordPass() {
    AnaplanService.validatePassword("password");
  }

  @Test
  public void validatePasswordThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(IllegalArgumentException.class, () -> AnaplanService.validatePassword(""));
    assertTrue(thrown.getMessage().contains("Anaplan password is required."));
  }

  @Test
  public void validateWorkspaceIdPass() {
    AnaplanService.validateWorkspaceId("workspaceId");
  }

  @Test
  public void validateWorkspaceIdThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(IllegalArgumentException.class, () -> AnaplanService.validateWorkspaceId(""));
    assertTrue(thrown.getMessage().contains("Anaplan workspaceId is required."));
  }

  @Test
  public void validateModelIdPass() {
    AnaplanService.validateWorkspaceId("modelId");
  }

  @Test
  public void validateModelIdThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(IllegalArgumentException.class, () -> AnaplanService.validateModelId(""));
    assertTrue(thrown.getMessage().contains("Anaplan modelId is required."));
  }

  @Test
  public void validateServiceLocationPass() {
    AnaplanService.validateServiceLocation("https://mock.anaplan.com");
  }

  @Test
  public void validateServiceLocationThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(
        IllegalArgumentException.class,
        () -> AnaplanService.validateServiceLocation("invalid URI"));
    assertTrue(thrown.getMessage().contains("Service location URI is invalid"));
  }

  @Test
  public void validateAuthServiceLocationPass() {
    AnaplanService.validateAuthServiceLocation("https://mock.anaplan.com");
  }

  @Test
  public void validateAuthServiceLocationThrowsIllegalArgumentException() {
    IllegalArgumentException thrown =
      assertThrows(
        IllegalArgumentException.class,
        () -> AnaplanService.validateAuthServiceLocation("invalid URI"));
    assertTrue(thrown.getMessage().contains("Authentication service URI is invalid"));
  }
}

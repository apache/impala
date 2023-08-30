/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.authorization.ranger;

import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.impala.authorization.AuthorizationTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;

import org.junit.Assert;
import org.junit.Test;


/**
 * This class contains unit tests for class RangerImpaladAuthorizationManager.
 */
public class RangerImpaladAuthorizationManagerTest extends AuthorizationTestBase {
  private final RangerImpaladAuthorizationManager rangerImpaladAuthzManager_;

  public RangerImpaladAuthorizationManagerTest() throws ImpalaException {
    super(AuthorizationProvider.RANGER);
    rangerImpaladAuthzManager_ =
        (RangerImpaladAuthorizationManager) super.authzFrontend_.getAuthzManager();
  }

  private static TShowRolesParams createTestShowRolesParams(boolean isShowCurrentRole) {
    TShowRolesParams showRolesParams = new TShowRolesParams(isShowCurrentRole);
    if (!isShowCurrentRole) {
      showRolesParams.setRequesting_user("admin");
    } else {
      showRolesParams.setRequesting_user("non_owner");
    }
    return showRolesParams;
  }

  private static TShowRolesParams createTestShowRolesParams(String grantGroup) {
    TShowRolesParams showRolesParams = createTestShowRolesParams(false);
    showRolesParams.setGrant_group(grantGroup);
    return showRolesParams;
  }

  private void _testNoExceptionInShowRolesWhenNoRolesInRanger(
      TShowRolesParams showRolesParams, String desc) {
    try {
      TShowRolesResult result = rangerImpaladAuthzManager_.getRoles(showRolesParams);
      Assert.assertNotNull(result.getRole_names());
    } catch (ImpalaException ex) {
      Assert.fail("No Exception should be thrown for show role statement:" + desc);
    }
  }

  @Test
  public void testNoExceptionInShowRolesWhenNoRolesInRanger() {
    // test SHOW ROLES statement.
    TShowRolesParams showRolesParams = createTestShowRolesParams(false);
    _testNoExceptionInShowRolesWhenNoRolesInRanger(showRolesParams, "SHOW ROLES");
    // test SHOW CURRENT ROLES statement.
    TShowRolesParams showCurrentRolesParams = createTestShowRolesParams(true);
    _testNoExceptionInShowRolesWhenNoRolesInRanger(
        showCurrentRolesParams, "SHOW CURRENT ROLES");
    // test SHOW ROLE GRANT GROUP <group name> statement.
    TShowRolesParams showRolesGrantGroupParams = createTestShowRolesParams("admin");
    _testNoExceptionInShowRolesWhenNoRolesInRanger(
        showRolesGrantGroupParams, "SHOW ROLE GRANT GROUP admin");
  }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.ranger.RangerAuthorizationFactory;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuthorizationUtilTest {
  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // since some test cases will need to modify the (static)
    // be flags, we need to save the original values so they
    // can be restored and not break other tests
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  private static BackendConfig authCfg(String authorization_factory_class,
      String authorization_provider) {
    final TBackendGflags stubCfg = new TBackendGflags();

    stubCfg.setAuthorization_factory_class(authorization_factory_class);
    stubCfg.setAuthorization_provider(authorization_provider);
    BackendConfig.create(stubCfg);
    return BackendConfig.INSTANCE;
  }

  private static BackendConfig authCfg(
      Class<? extends AuthorizationFactory> authorization_factory_class,
      String authorization_provider) {
    return authCfg(authorization_factory_class.getCanonicalName(),
        authorization_provider);
  }

  @Test
  public void testAuthorizationProviderFlag()
      throws Exception {
    // Use authorization provider if authorization factory class not set
    assertEquals(RangerAuthorizationFactory.class.getCanonicalName(),
        AuthorizationUtil.authzFactoryClassNameFrom(authCfg("", "ranger")));
    assertEquals(NoopAuthorizationFactory.class.getCanonicalName(),
        AuthorizationUtil.authzFactoryClassNameFrom(authCfg("", "")));

    // Authorization factory class overrides authorization provider
    assertEquals(RangerAuthorizationFactory.class.getCanonicalName(),
        AuthorizationUtil.authzFactoryClassNameFrom(
            authCfg(RangerAuthorizationFactory.class, "")));
  }
}

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

package org.apache.impala.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;

/**
 * An implementation of {@link AuthorizationFactory} that does not do any
 * authorization. This is the default implementation when authorization is disabled.
 */
public class NoneAuthorizationFactory implements AuthorizationFactory {
  private final AuthorizationConfig authzConfig_;

  public NoneAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = newAuthorizationConfig(backendConfig);
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public NoneAuthorizationFactory() {
    authzConfig_ = disabledAuthorizationConfig();
  }

  private static AuthorizationConfig disabledAuthorizationConfig() {
    return new AuthorizationConfig() {
      @Override
      public boolean isEnabled() { return false; }
      @Override
      public AuthorizationProvider getProvider() { return AuthorizationProvider.NONE; }
      @Override
      public String getServerName() { return null; }
    };
  }

  @Override
  public AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig) {
    return disabledAuthorizationConfig();
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    return new AuthorizationChecker(authzConfig_) {
      @Override
      protected boolean authorize(User user, PrivilegeRequest request)
          throws InternalException {
        return true;
      }
    };
  }
}

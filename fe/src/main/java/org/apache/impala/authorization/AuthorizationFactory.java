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

import org.apache.impala.service.BackendConfig;

/**
 * An interface for building an authorization provider. The implementation of
 * this interface needs to have a constructor that takes {@link BackendConfig}.
 */
public interface AuthorizationFactory {
  AuthorizableFactory DEFAULT_AUTHORIZABLE_FACTORY = new DefaultAuthorizableFactory();

  /**
   * Creates a new {@link AuthorizationFactory}.
   */
  AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig);

  /**
   * Gets an instance of {@link AuthorizationConfig}.
   */
  AuthorizationConfig getAuthorizationConfig();

  /**
   * Creates a new instance of {@link AuthorizationChecker}.
   */
  AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy);

  /**
   * Creates a new instance of {@link AuthorizationChecker}.
   */
  default AuthorizationChecker newAuthorizationChecker() {
    return newAuthorizationChecker(null);
  }

  /**
   * Gets an instance of {@link AuthorizableFactory}.
   */
  default AuthorizableFactory getAuthorizableFactory() {
    return DEFAULT_AUTHORIZABLE_FACTORY;
  }
}

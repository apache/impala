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

import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeCatalogManager;

import java.util.function.Supplier;

/**
 * An interface for building an authorization provider. The implementation of
 * this interface needs to have a constructor that takes {@link BackendConfig}.
 */
public interface AuthorizationFactory {
  AuthorizableFactory DEFAULT_AUTHORIZABLE_FACTORY = new DefaultAuthorizableFactory();

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

  /**
   * Creates a new instance of {@link AuthorizationManager}.
   *
   * NOTE: Do not hold the underlying object of {@link AuthorizationChecker} since the
   * object may become stale.
   */
  AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      Supplier<? extends AuthorizationChecker> authzChecker) throws ImpalaException;

  /**
   * Creates a new instance of {@link AuthorizationManager}.
   */
  AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog)
      throws ImpalaException;

  /**
   * Returns whether the authorization implementation supports column masking and row
   * filtering. Currently, only Ranger implementation supports these.
   */
  boolean supportsTableMasking();
}

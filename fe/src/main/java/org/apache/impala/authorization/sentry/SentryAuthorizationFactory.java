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

package org.apache.impala.authorization.sentry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.impala.authorization.AuthorizableFactory;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeCatalogManager;

import java.util.function.Supplier;

/**
 * An implementation of {@link AuthorizationFactory} that uses Sentry.
 */
public class SentryAuthorizationFactory implements AuthorizationFactory {
  private static final SentryAuthorizableFactory AUTHORIZABLE_FACTORY =
      new SentryAuthorizableFactory();

  private final AuthorizationConfig authzConfig_;

  public SentryAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = newAuthorizationConfig(backendConfig);
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public SentryAuthorizationFactory(AuthorizationConfig authzConfig) {
    Preconditions.checkNotNull(authzConfig);
    authzConfig_ = authzConfig;
  }

  private static AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig) {
    String serverName = backendConfig.getBackendCfg().getServer_name();
    String sentryConfigFile = backendConfig.getBackendCfg().getSentry_config();
    String policyProviderClassName = backendConfig.getBackendCfg()
        .getAuthorization_policy_provider_class();
    // The logic for creating Sentry authorization config is inconsistent between
    // catalogd and impalad. In catalogd, only --sentry_config flag is required to enable
    // authorization. In impalad, --server_name and --sentry_config are required.
    // Keeping the same logic for backward compatibility.
    if (Strings.isNullOrEmpty(serverName)) {
      // Check if the Sentry Service is configured. If so, create a configuration object.
      SentryConfig sentryConfig = new SentryConfig(null);
      if (!Strings.isNullOrEmpty(sentryConfigFile)) {
        sentryConfig = new SentryConfig(sentryConfigFile);
        sentryConfig.loadConfig();
      }
      return new SentryAuthorizationConfig(sentryConfig);
    }
    return new SentryAuthorizationConfig(serverName, sentryConfigFile,
        policyProviderClassName);
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    return new SentryAuthorizationChecker(authzConfig_, authzPolicy);
  }

  @Override
  public AuthorizableFactory getAuthorizableFactory() { return AUTHORIZABLE_FACTORY; }

  @Override
  public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      Supplier<? extends AuthorizationChecker> authzChecker) {
    return new SentryImpaladAuthorizationManager(catalog, authzChecker);
  }

  @Override
  public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog)
      throws ImpalaException {
    return new SentryCatalogdAuthorizationManager(
        (SentryAuthorizationConfig) getAuthorizationConfig(), catalog);
  }
}

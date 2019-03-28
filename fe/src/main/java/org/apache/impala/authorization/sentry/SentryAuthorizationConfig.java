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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;

/**
 * Impala authorization configuration with Sentry.
 */
public class SentryAuthorizationConfig implements AuthorizationConfig {
  private final String serverName_;
  private final SentryConfig sentryConfig_;
  private final String policyProviderClassName_;

  /**
   * Creates a new authorization configuration object.
   * @param serverName - The name of this Impala server.
   * @param sentryConfigFile - Absolute path and file name of the sentry service.
   * @param policyProviderClassName - Class name of the policy provider to use.
   */
  public SentryAuthorizationConfig(String serverName, String sentryConfigFile,
      String policyProviderClassName) {
    serverName_ = serverName;
    sentryConfig_ = new SentryConfig(sentryConfigFile);
    if (!Strings.isNullOrEmpty(policyProviderClassName)) {
      policyProviderClassName = policyProviderClassName.trim();
    }
    policyProviderClassName_ = policyProviderClassName;
    validateConfig();
  }

  public SentryAuthorizationConfig(SentryConfig config) {
    sentryConfig_ = config;
    serverName_ = null;
    policyProviderClassName_ = null;
  }

  /**
   * Returns an AuthorizationConfig object that has authorization disabled.
   */
  public static SentryAuthorizationConfig createAuthDisabledConfig() {
    return new SentryAuthorizationConfig(null, null, null);
  }

  /**
   * Returns an AuthorizationConfig object configured to use Hadoop user->group mappings
   * for the authorization provider.
   */
  public static SentryAuthorizationConfig createHadoopGroupAuthConfig(String serverName,
      String sentryConfigFile) {
    return new SentryAuthorizationConfig(serverName, sentryConfigFile,
        HadoopGroupResourceAuthorizationProvider.class.getName());
  }

  /*
   * Validates the authorization configuration and throws an AuthorizationException
   * if any problems are found. If authorization is disabled, config checks are skipped.
   */
  private void validateConfig() throws IllegalArgumentException {
    // If authorization is not enabled, config checks are skipped.
    if (!isEnabled()) return;

    // Only load the sentry configuration if a sentry-site.xml configuration file was
    // specified. It is optional for impalad.
    if (!Strings.isNullOrEmpty(sentryConfig_.getConfigFile())) {
      sentryConfig_.loadConfig();
    }

    if (Strings.isNullOrEmpty(serverName_)) {
      throw new IllegalArgumentException(
          "Authorization is enabled but the server name is null or empty. Set the " +
              "server name using the impalad --server_name flag.");
    }
    if (Strings.isNullOrEmpty(policyProviderClassName_)) {
      throw new IllegalArgumentException("Authorization is enabled but the " +
          "authorization policy provider class name is null or empty. Set the class " +
          "name using the --authorization_policy_provider_class impalad flag.");
    }

    Class<?> providerClass = null;
    try {
      // Get the Class object without performing any initialization.
      providerClass = Class.forName(policyProviderClassName_, false,
          this.getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(String.format("The authorization policy " +
          "provider class '%s' was not found.", policyProviderClassName_), e);
    }
    Preconditions.checkNotNull(providerClass);
    if (!ResourceAuthorizationProvider.class.isAssignableFrom(providerClass)) {
      throw new IllegalArgumentException(String.format("The authorization policy " +
              "provider class '%s' must be a subclass of '%s'.",
          policyProviderClassName_,
          ResourceAuthorizationProvider.class.getName()));
    }
  }

  /**
   * Returns true if authorization is enabled.
   * If either serverName_, policyFile_, or sentryConfig_ file is set (not null
   * or empty), authorization is considered enabled.
   */
  @Override
  public boolean isEnabled() {
    return !Strings.isNullOrEmpty(serverName_) ||
        !Strings.isNullOrEmpty(sentryConfig_.getConfigFile());
  }

  @Override
  public String getProviderName() { return "sentry"; }

  /**
   * The server name to secure.
   */
  @Override
  public String getServerName() { return serverName_; }

  /**
   * The Sentry configuration.
   */
  public SentryConfig getSentryConfig() { return sentryConfig_; }

  /**
   * Returns Sentry policy provider class name.
   */
  public String getPolicyProviderClassName() { return policyProviderClassName_; }
}

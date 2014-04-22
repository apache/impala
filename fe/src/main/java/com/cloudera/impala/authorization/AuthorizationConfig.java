// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.authorization;

import com.google.common.base.Strings;

/*
 * Class that contains configuration details for Impala authorization.
 */
public class AuthorizationConfig {
  private final String serverName_;
  // Set only if the policy provider is file-based.
  private final String policyFile_;
  private final SentryConfig sentryConfig_;

  public AuthorizationConfig(String serverName, String policyFile,
      String sentryConfigFile) {
    serverName_ = serverName;
    policyFile_ = policyFile;
    sentryConfig_ = new SentryConfig(sentryConfigFile);
  }

  /**
   * Returns an AuthorizationConfig object that has authorization disabled.
   */
  public static AuthorizationConfig createAuthDisabledConfig() {
    return new AuthorizationConfig(null, null, null);
  }

  /*
   * Validates the authorization configuration and throws an AuthorizationException
   * if any problems are found. If authorization is disabled, config checks are skipped.
   */
  public void validateConfig() throws IllegalArgumentException {
    // If authorization is not enabled, config checks are skipped.
    if (!isEnabled()) {
      return;
    }

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
  }

  /**
   * Returns true if authorization is enabled.
   * If either serverName, policyFile, or sentryServiceConfig_ file is set (not null
   * or empty), authorization is considered enabled.
   */
  public boolean isEnabled() {
    return !Strings.isNullOrEmpty(serverName_) || !Strings.isNullOrEmpty(policyFile_) ||
        !Strings.isNullOrEmpty(sentryConfig_.getConfigFile());
  }

  /**
   * Returns true if using an authorization policy from a file in HDFS. If false,
   * uses an authorization policy based on cached metadata sent from the catalog server
   * via the statestore.
   */
  public boolean isFileBasedPolicy() { return !Strings.isNullOrEmpty(policyFile_); }

  /**
   * The server name to secure.
   */
  public String getServerName() { return serverName_; }

  /**
   * The policy file path.
   */
  public String getPolicyFile() { return policyFile_; }

  /**
   * The Sentry configuration.
   */
  public SentryConfig getSentryConfig() { return sentryConfig_; }
}

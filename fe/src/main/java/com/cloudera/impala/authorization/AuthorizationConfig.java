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


/*
 * Class that contains configuration details for Impala authorization.
 */
public class AuthorizationConfig {
  private final String serverName;
  private final String policyFile;

  public AuthorizationConfig(String serverName, String policyFile) {
    this.serverName = serverName;
    this.policyFile = policyFile;
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

    if (serverName == null || serverName.isEmpty()) {
      throw new IllegalArgumentException("Authorization is enabled but the server name" +
          " is null or empty. Set the server name using the impalad --server_name flag."
          );
    }
    if (policyFile == null || policyFile.isEmpty()) {
      throw new IllegalArgumentException("Authorization is enabled but the policy file" +
          " path was null or empty. Set the policy file using the " +
          "--authorization_policy_file impalad flag.");
    }
  }

  /*
   * Returns true if authorization is enabled.
   * If any of the authorization config values is set to a non-empty value authorization
   * is considered enabled.
   */
  public boolean isEnabled() {
    return (serverName != null && !serverName.isEmpty()) ||
           (policyFile != null && !policyFile.isEmpty());
  }

  /*
   * The server name to secure.
   */
  public String getServerName() {
    return serverName;
  }

  /*
   * The policy file path.
   */
  public String getPolicyFile() {
    return policyFile;
  }

  /*
   * Returns an AuthorizationConfig object that has authorization disabled.
   */
  public static AuthorizationConfig createAuthDisabledConfig() {
    return new AuthorizationConfig(null, null);
  }
}
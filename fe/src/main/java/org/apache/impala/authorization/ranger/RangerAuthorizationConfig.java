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

package org.apache.impala.authorization.ranger;

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;

/**
 * Impala authorization config with Ranger.
 */
public class RangerAuthorizationConfig implements AuthorizationConfig {
  private final String serviceType_;
  private final String appId_;
  private final String serverName_;
  private final String clusterName_;
  private final String clusterType_;
  private final RangerPolicyEngineOptions policyEngineOptions_;
  private final RangerPluginConfig rangerConfig_;

  public RangerAuthorizationConfig(String serviceType, String appId, String serverName,
      String clusterName, String clusterType,
      RangerPolicyEngineOptions policyEngineOptions) {
    serviceType_ = Preconditions.checkNotNull(serviceType);
    appId_ = Preconditions.checkNotNull(appId);
    serverName_ = Preconditions.checkNotNull(serverName);
    clusterName_ = clusterName;
    clusterType_ = clusterType;
    policyEngineOptions_ = policyEngineOptions;
    rangerConfig_ = new RangerPluginConfig(serviceType_, serverName_, appId_,
        clusterName_, clusterType_, policyEngineOptions_);
  }

  @Override
  public boolean isEnabled() { return true; }

  @Override
  public String getProviderName() { return "ranger"; }

  @Override
  public String getServerName() { return serverName_; }

  /**
   * Returns the Ranger service type.
   */
  public String getServiceType() { return serviceType_; }

  /**
   * Returns the Ranger application ID.
   */
  public String getAppId() { return appId_; }

  /**
   * Returns the Ranger cluster name.
   */
  public String getClusterName() { return clusterName_; }

  /**
   * Returns the Ranger cluster type.
   */
  public String getClusterType() { return clusterType_; }

  /**
   * Returns the Ranger policy engine options.
   */
  public RangerPolicyEngineOptions getPolicyEngineOptions() {
      return policyEngineOptions_;
  }

  public RangerPluginConfig getRangerConfig() { return rangerConfig_; }
}

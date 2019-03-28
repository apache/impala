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
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

/**
 * Impala authorization config with Ranger.
 */
public class RangerAuthorizationConfig implements AuthorizationConfig {
  private final String serviceType_;
  private final String appId_;
  private final String serverName_;
  private final RangerConfiguration rangerConfig_;

  public RangerAuthorizationConfig(String serviceType, String appId, String serverName) {
    serviceType_ = Preconditions.checkNotNull(serviceType);
    appId_ = Preconditions.checkNotNull(appId);
    serverName_ = Preconditions.checkNotNull(serverName);
    rangerConfig_ = RangerConfiguration.getInstance();
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

  public RangerConfiguration getRangerConfig() { return rangerConfig_; }
}

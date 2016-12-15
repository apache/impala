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

import java.io.IOException;

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.RuntimeEnv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Class that represents a User of an Impala session.
 */
public class User {

  private final String name_;

  // Refer to BackendConfig.initAuthToLocal() for initialization
  // of static auth_to_local configs in KerberosName class.
  private final KerberosName kerberosName_;

  public User(String name) {
    Preconditions.checkNotNull(name);
    name_ = name;
    this.kerberosName_ = new KerberosName(name);
  }

  public String getName() { return name_; }

  public String getShortName() throws InternalException {
    try {
      return kerberosName_.getShortName();
    } catch (IOException e) {
      throw new InternalException(
          "Error calling getShortName() for user: " + getName(), e);
    }
  }

  /*
   * Returns the shortname for the user after applying auth_to_local
   * rules from string 'rules'. This is exposed for testing purposes only.
   * Ideally these rules are populated from hdfs configuration files.
   */
  @VisibleForTesting
  public String getShortNameForTesting(String rules) {
    Preconditions.checkNotNull(rules);
    Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
    String currentRules = KerberosName.getRules();
    KerberosName.setRules(rules);
    String shortName = null;
    try {
      shortName = getShortName();
    } catch (InternalException e) {
      e.printStackTrace();
    }
    // reset the rules
    KerberosName.setRules(currentRules);
    return shortName;
  }

  @VisibleForTesting
  public static void setRulesForTesting(String rules) {
    Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
    KerberosName.setRules(rules);
  }
}

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

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.service.BackendConfig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;
import org.apache.hadoop.security.authentication.util.KerberosName;

/*
 * Class that represents a User of an Impala session.
 */
public class User {

  static {
    // If auth_to_local is enabled, we read the configuration hadoop.security.auth_to_local
    // from core-site.xml and use it for principal to short name conversion. If it is not,
    // we use the defaultRule ("RULE:[1:$1] RULE:[2:$1]"), which just extracts the user
    // name from any principal of form a@REALM or a/b@REALM. If auth_to_local is enabled
    // and hadoop.security.auth_to_local is not specified in the hadoop configs, we use
    // the "DEFAULT" rule that just extracts the username from any principal in the
    // cluster's local realm. For more details on principal to short name translation,
    // refer to org.apache.hadoop.security.KerberosName.
    final String defaultRule = "RULE:[1:$1] RULE:[2:$1]";
    final Configuration conf = new Configuration();
    if (BackendConfig.isAuthToLocalEnabled()) {
      KerberosName.setRules(conf.get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
    } else {
      // just extract the simple user name
      KerberosName.setRules(defaultRule);
    }
  }

  private final String name_;

  private KerberosName kerberosName_;

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

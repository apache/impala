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

package org.apache.impala.analysis;

import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.impala.authorization.User;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Strings;

/**
 * Base class for all authorization statements - CREATE/DROP/SHOW ROLE, GRANT/REVOKE
 * ROLE/privilege, etc.
 */
public class AuthorizationStmt extends StatementBase {
  // Set during analysis
  protected User requestingUser_;

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    AuthorizationConfig authzConfig = analyzer.getAuthzConfig();
    if (!authzConfig.isEnabled()) {
      throw new AnalysisException("Authorization is not enabled. To enable " +
          "authorization restart Impala with the --server_name=<name> flag.");
    }
    if (Strings.isNullOrEmpty(analyzer.getUser().getName())) {
      throw new AnalysisException("Cannot execute authorization statement with an " +
          "empty username.");
    }
    requestingUser_ = analyzer.getUser();
  }
}

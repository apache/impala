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

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.User;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TKillQueryReq;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.TUniqueIdUtil;

public final class KillQueryStmt extends StatementBase {
  private final String queryIdString_;
  private TUniqueId queryId_;
  private User requestingUser_;
  private boolean requestedByAdmin_ = true;

  public KillQueryStmt(String queryId) {
    queryIdString_ = queryId;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return String.format("KILL QUERY '%s'", queryIdString_);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    try {
      queryId_ = TUniqueIdUtil.ParseId(queryIdString_);
    } catch (NumberFormatException e) {
      throw new AnalysisException(
          String.format("Invalid query id: '%s'", queryIdString_));
    }
    requestingUser_ = analyzer.getUser();
    AuthorizationConfig authzConfig = analyzer.getAuthzConfig();
    if (authzConfig.isEnabled()) {
      // Check whether the user is an admin (i.e. user with ALL privilege on server).
      String authzServer = authzConfig.getServerName();
      Preconditions.checkNotNull(authzServer);
      analyzer.setMaskPrivChecks(null);
      analyzer.registerPrivReq(builder -> builder.onServer(authzServer).all().build());
    }
  }

  public TKillQueryReq toThrift() {
    return new TKillQueryReq(queryId_, requestingUser_.getName(), requestedByAdmin_);
  }

  @Override
  public void handleAuthorizationException(AnalysisResult analysisResult) {
    requestedByAdmin_ = false;

    // By default, a user will not be authorized to access the runtime profile if a
    // masked privilege request fails. This will disallow any non-admin user to access
    // the runtime profile even if the user can kill the query.
    //
    // Set to true to allow non-admin users to access the runtime profile.
    analysisResult.setUserHasProfileAccess(true);
  }
}

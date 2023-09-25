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
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterDbType;
import org.apache.impala.thrift.TOwnerType;
import org.apache.impala.util.MetaStoreUtil;

/**
 * Represents an ALTER DATABASE db SET OWNER [USER|ROLE] owner statement.
 */
public class AlterDbSetOwnerStmt extends AlterDbStmt {
  private final Owner owner_;

  // Server name needed for privileges. Set during analysis.
  private String serverName_;

  public AlterDbSetOwnerStmt(String dbName, Owner owner) {
    super(dbName);
    Preconditions.checkNotNull(owner);
    owner_ = owner;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Require ALL with GRANT OPTION privilege.
    analyzer.getDb(dbName_, Privilege.ALL, /* throw if does not exist */ true,
        /* grant option */ true);
    String ownerName = owner_.getOwnerName();
    if (ownerName.length() > MetaStoreUtil.MAX_OWNER_LENGTH) {
      throw new AnalysisException(String.format("Owner name exceeds maximum length of " +
          "%d characters. The given owner name has %d characters.",
          MetaStoreUtil.MAX_OWNER_LENGTH, ownerName.length()));
    }
    // We don't allow assigning to a non-existent role because Ranger should know about
    // all roles. Ranger does not track all users so we allow assigning to a user
    // that Ranger doesn't know about yet.
    if (analyzer.isAuthzEnabled() && owner_.getOwnerType() == TOwnerType.ROLE) {
      AuthorizationFactory authzFactory = analyzer.getAuthzFactory();
      AuthorizationChecker authzChecker = authzFactory.newAuthorizationChecker();
      if (!authzChecker.roleExists(ownerName)) {
        throw new AnalysisException(
            String.format("Role '%s' does not exist.", ownerName));
      }
    }
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();
  }

  @Override
  public TAlterDbParams toThrift() {
    TAlterDbParams params = super.toThrift();
    params.setAlter_type(TAlterDbType.SET_OWNER);
    TAlterDbSetOwnerParams setOwnerParams = new TAlterDbSetOwnerParams();
    setOwnerParams.setOwner_type(owner_.getOwnerType());
    setOwnerParams.setOwner_name(owner_.getOwnerName());
    setOwnerParams.setServer_name(serverName_);
    params.setSet_owner_params(setOwnerParams);
    return params;
  }
}

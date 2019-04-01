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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeCatalogManager;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.util.ClassUtil;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An implementation of {@link AuthorizationFactory} that does not do any
 * authorization. This is the default implementation when authorization is disabled.
 */
public class NoneAuthorizationFactory implements AuthorizationFactory {
  private final AuthorizationConfig authzConfig_;

  public NoneAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = newAuthorizationConfig(backendConfig);
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public NoneAuthorizationFactory() {
    authzConfig_ = disabledAuthorizationConfig();
  }

  private static AuthorizationConfig disabledAuthorizationConfig() {
    return new AuthorizationConfig() {
      @Override
      public boolean isEnabled() { return false; }
      @Override
      public AuthorizationProvider getProvider() { return AuthorizationProvider.NONE; }
      @Override
      public String getServerName() { return null; }
    };
  }

  public static class NoneAuthorizationManager implements AuthorizationManager {
    @Override
    public boolean isAdmin(User user) throws ImpalaException {
      return false;
    }

    @Override
    public void createRole(User requestingUser, TCreateDropRoleParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void dropRole(User requestingUser, TCreateDropRoleParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantPrivilegeToRole(User requestingUser, TGrantRevokePrivParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromRole(User requestingUser,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantPrivilegeToUser(User requestingUser, TGrantRevokePrivParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromUser(User requestingUser,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantPrivilegeToGroup(User requestingUser, TGrantRevokePrivParams params,
        TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromGroup(User requestingUser,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public TResultSet getPrivileges(TShowGrantPrincipalParams params)
        throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void updateDatabaseOwnerPrivilege(String serverName, String databaseName,
        String oldOwner, PrincipalType oldOwnerType, String newOwner,
        PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void updateTableOwnerPrivilege(String serverName, String databaseName,
        String tableName, String oldOwner, PrincipalType oldOwnerType, String newOwner,
        PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }
  }

  @Override
  public AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig) {
    return disabledAuthorizationConfig();
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    return new AuthorizationChecker(authzConfig_) {
      @Override
      protected boolean authorize(User user, PrivilegeRequest request)
          throws InternalException {
        return true;
      }

      @Override
      public Set<String> getUserGroups(User user) throws InternalException {
        return Collections.emptySet();
      }
    };
  }

  @Override
  public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      Supplier<? extends AuthorizationChecker> authzChecker) {
    return new NoneAuthorizationManager();
  }

  @Override
  public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog) {
    return new NoneAuthorizationManager();
  }
}

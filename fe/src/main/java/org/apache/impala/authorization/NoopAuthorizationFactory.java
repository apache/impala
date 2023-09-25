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
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.util.ClassUtil;
import org.apache.impala.util.EventSequence;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An implementation of {@link AuthorizationFactory} that does not do any
 * authorization. This is the default implementation when authorization is disabled.
 */
public class NoopAuthorizationFactory implements AuthorizationFactory {
  private final AuthorizationConfig authzConfig_;

  public NoopAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = disabledAuthorizationConfig();
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public NoopAuthorizationFactory() {
    authzConfig_ = disabledAuthorizationConfig();
  }

  private static AuthorizationConfig disabledAuthorizationConfig() {
    return new AuthorizationConfig() {
      @Override
      public boolean isEnabled() { return false; }
      @Override
      public String getProviderName() { return "noop"; }
      @Override
      public String getServerName() { return null; }
    };
  }

  public static class NoopAuthorizationManager implements AuthorizationManager {
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
    public void grantPrivilegeToRole(TCatalogServiceRequestHeader header,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromRole(TCatalogServiceRequestHeader header,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantPrivilegeToUser(TCatalogServiceRequestHeader header,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromUser(TCatalogServiceRequestHeader header,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void grantPrivilegeToGroup(TCatalogServiceRequestHeader header,
        TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
      throw new UnsupportedOperationException(String.format("%s is not supported",
          ClassUtil.getMethodName()));
    }

    @Override
    public void revokePrivilegeFromGroup(TCatalogServiceRequestHeader header,
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

    @Override
    public AuthorizationDelta refreshAuthorization(boolean resetVersions) {
      return new AuthorizationDelta();
    }
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    return new BaseAuthorizationChecker(authzConfig_) {
      @Override
      protected boolean authorizeResource(AuthorizationContext authzCtx, User user,
          PrivilegeRequest request) throws InternalException {
        return true;
      }

      @Override
      public Set<String> getUserGroups(User user) throws InternalException {
        return Collections.emptySet();
      }

      @Override
      protected void authorizeRowFilterAndColumnMask(User user,
          List<PrivilegeRequest> privilegeRequests)
          throws AuthorizationException, InternalException {
      }

      @Override
      public void invalidateAuthorizationCache() {}

      @Override
      public AuthorizationContext createAuthorizationContext(boolean doAudits,
          String sqlStmt, TSessionState sessionState, Optional<EventSequence> timeline) {
        return new AuthorizationContext(timeline);
      }

      @Override
      public boolean needsMaskingOrFiltering(User user, String dbName,
          String tableName, List<String> requiredColumns) {
        return false;
      }

      @Override
      public boolean needsRowFiltering(User user, String dbName, String tableName) {
        return false;
      }

      @Override
      public String createColumnMask(User user, String dbName, String tableName,
          String columnName, AuthorizationContext authzCtx) {
        return null;
      }

      @Override
      public String createRowFilter(User user, String dbName, String tableName,
          AuthorizationContext rangerCtx) {
        return null;
      }

      @Override
      public void postAnalyze(AuthorizationContext authzCtx) {
      }

      @Override
      public boolean roleExists(String roleName) {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", ClassUtil.getMethodName()));
      }
    };
  }

  @Override
  public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      Supplier<? extends AuthorizationChecker> authzChecker) {
    return new NoopAuthorizationManager();
  }

  @Override
  public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog) {
    return new NoopAuthorizationManager();
  }

  @Override
  public boolean supportsTableMasking() {
    return false;
  }
}

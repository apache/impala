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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.util.ClassUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An implementation of {@link AuthorizationManager} for Catalogd using Ranger.
 *
 * Operations here make requests to Ranger via the {@link RangerImpalaPlugin} to
 * manage privileges for users.
 *
 * Operations not supported by Ranger will throw an {@link UnsupportedOperationException}.
 */
public class RangerCatalogdAuthorizationManager implements AuthorizationManager {
  private final RangerDefaultAuditHandler auditHandler_;
  private final Supplier<RangerImpalaPlugin> plugin_;

  public RangerCatalogdAuthorizationManager(Supplier<RangerImpalaPlugin> pluginSupplier) {
    auditHandler_ = new RangerDefaultAuditHandler();
    plugin_ = pluginSupplier;
  }

  @Override
  public void createRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToUser(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        requestingUser.getName(), params.getPrincipal_name(), Collections.emptyList(),
        plugin_.get().getClusterName(), params.getPrivileges());

    grantPrivilege(requests);
  }

  @Override
  public void revokePrivilegeFromUser(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        requestingUser.getName(), params.getPrincipal_name(), Collections.emptyList(),
        plugin_.get().getClusterName(), params.getPrivileges());

    revokePrivilege(requests);
  }

  @Override
  public void grantPrivilegeToGroup(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        requestingUser.getName(), null,
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), params.getPrivileges());

    grantPrivilege(requests);
  }

  @Override
  public void revokePrivilegeFromGroup(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        requestingUser.getName(), null,
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), params.getPrivileges());

    revokePrivilege(requests);
  }

  @VisibleForTesting
  public void grantPrivilege(List<GrantRevokeRequest> requests) throws ImpalaException {
    try {
      for (GrantRevokeRequest request : requests) {
        plugin_.get().grantAccess(request, auditHandler_);
      }
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }
  }

  @VisibleForTesting
  public void revokePrivilege(List<GrantRevokeRequest> requests) throws ImpalaException {
    try {
      for (GrantRevokeRequest request : requests) {
        plugin_.get().revokeAccess(request, auditHandler_);
      }
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }
  }

  @Override
  public TResultSet getPrivileges(TShowGrantPrincipalParams params)
      throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void updateDatabaseOwnerPrivilege(String serverName, String databaseName,
      String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
  }

  @Override
  public void updateTableOwnerPrivilege(String serverName, String databaseName,
      String tableName, String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException {
  }

  @Override
  public AuthorizationDelta refreshAuthorization(boolean resetVersions) {
    // TODO: IMPALA-8293 (part 2)
    return new AuthorizationDelta();
  }

  public static List<GrantRevokeRequest> createGrantRevokeRequests(String grantor,
      String user, List<String> groups, String clusterName, List<TPrivilege> privileges) {
    List<GrantRevokeRequest> requests = new ArrayList<>();

    for (TPrivilege p: privileges) {
      Function<Map<String, String>, GrantRevokeRequest> createRequest = (resource) ->
          createGrantRevokeRequest(grantor, user, groups, clusterName, p.has_grant_opt,
              p.privilege_level, resource);

      // Ranger Impala service definition defines 3 resources:
      // [DB -> Table -> Column]
      // [DB -> Function]
      // [URI]
      // What it means is if we grant a particular privilege on a resource that
      // is common to other resources, we need to grant that privilege to those
      // resources.
      if (p.getColumn_name() != null || p.getTable_name() != null) {
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
      } else if (p.getUri() != null) {
        requests.add(createRequest.apply(RangerUtil.createUriResource(p)));
      } else if (p.getDb_name() != null) {
        // DB is used by column and function resources.
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
        requests.add(createRequest.apply(RangerUtil.createFunctionResource(p)));
      } else {
        // Server is used by column, function, and URI resources.
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
        requests.add(createRequest.apply(RangerUtil.createFunctionResource(p)));
        requests.add(createRequest.apply(RangerUtil.createUriResource(p)));
      }
    }

    return requests;
  }

  private static GrantRevokeRequest createGrantRevokeRequest(String grantor, String user,
      List<String> groups, String clusterName, boolean withGrantOpt,
      TPrivilegeLevel level, Map<String, String> resource) {
    GrantRevokeRequest request = new GrantRevokeRequest();
    request.setGrantor(grantor);
    if (user != null) request.getUsers().add(user);
    if (!groups.isEmpty()) request.getGroups().addAll(groups);
    request.setDelegateAdmin((withGrantOpt) ? Boolean.TRUE : Boolean.FALSE);
    request.setEnableAudit(Boolean.TRUE);
    request.setReplaceExistingPermissions(Boolean.FALSE);
    request.setClusterName(clusterName);
    request.setResource(resource);

    if (level == TPrivilegeLevel.INSERT) {
      request.getAccessTypes().add(RangerAuthorizationChecker.UPDATE_ACCESS_TYPE);
    } else if (level == TPrivilegeLevel.REFRESH) {
      // TODO: this is a hack. It will need to be fixed once refresh is added into Hive
      // service definition.
      request.getAccessTypes().add(RangerAuthorizationChecker.REFRESH_ACCESS_TYPE);
    } else {
      request.getAccessTypes().add(level.name().toLowerCase());
    }

    return request;
  }
}

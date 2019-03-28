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
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.*;
import org.apache.impala.util.ClassUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
  public boolean isAdmin(User user) throws ImpalaException {
    return false;
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
        requestingUser.getName(), params.getPrincipal_name(),
        plugin_.get().getClusterName(), params.getPrivileges());

    grantPrivilegeToUser(requestingUser, requests, response);
  }

  @VisibleForTesting
  public void grantPrivilegeToUser(User requestingUser, List<GrantRevokeRequest> requests,
      TDdlExecResponse response) throws ImpalaException {
    try {
      for (GrantRevokeRequest request : requests) {
        plugin_.get().grantAccess(request, auditHandler_);
      }
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }
  }

  @Override
  public void revokePrivilegeFromUser(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        requestingUser.getName(), params.getPrincipal_name(),
        plugin_.get().getClusterName(), params.getPrivileges());

    revokePrivilegeFromUser(requestingUser, requests, response);
  }

  @VisibleForTesting
  public void revokePrivilegeFromUser(User requestingUser,
      List<GrantRevokeRequest> requests, TDdlExecResponse response)
      throws ImpalaException {
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

  public static List<GrantRevokeRequest> createGrantRevokeRequests(String grantor,
      String user, String clusterName, List<TPrivilege> privileges) {
    List<GrantRevokeRequest> requests = new ArrayList<>();

    for (TPrivilege p: privileges) {
      Function<Map<String, String>, GrantRevokeRequest> createRequest = (resource) ->
          createGrantRevokeRequest(grantor, user, clusterName, p.privilege_level,
              resource);

      // Ranger Impala service definition defines 3 resources:
      // [DB -> Table -> Column]
      // [DB -> Function]
      // [URI]
      // What it means is if we grant a particular privilege on a resource that
      // is common to other resources, we need to grant that privilege to those
      // resources.
      if (p.getColumn_name() != null || p.getTable_name() != null) {
        requests.add(createRequest.apply(createColumnResource(p)));
      } else if (p.getUri() != null) {
        requests.add(createRequest.apply(createUriResource(p)));
      } else if (p.getDb_name() != null) {
        // DB is used by column and function resources.
        requests.add(createRequest.apply(createColumnResource(p)));
        requests.add(createRequest.apply(createFunctionResource(p)));
      } else {
        // Server is used by column, function, and URI resources.
        requests.add(createRequest.apply(createColumnResource(p)));
        requests.add(createRequest.apply(createFunctionResource(p)));
        requests.add(createRequest.apply(createUriResource(p)));
      }
    }

    return requests;
  }

  private static GrantRevokeRequest createGrantRevokeRequest(String grantor, String user,
      String clusterName, TPrivilegeLevel level, Map<String, String> resource) {
    GrantRevokeRequest request = new GrantRevokeRequest();
    request.setGrantor(grantor);
    request.getUsers().add(user);
    request.setDelegateAdmin(Boolean.FALSE);
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

  private static Map<String, String> createColumnResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.TABLE, getOrAll(privilege.getTable_name()));
    resource.put(RangerImpalaResourceBuilder.COLUMN,
        getOrAll(privilege.getColumn_name()));

    return resource;
  }

  private static Map<String, String> createUriResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();
    String uri = privilege.getUri();
    resource.put(RangerImpalaResourceBuilder.URL, uri == null ? "*" : uri);

    return resource;
  }

  private static Map<String, String> createFunctionResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.UDF, "*");

    return resource;
  }

  private static String getOrAll(String resource) {
    return (resource == null) ? "*" : resource;
  }
}

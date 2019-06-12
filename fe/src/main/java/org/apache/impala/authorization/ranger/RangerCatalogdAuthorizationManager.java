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
import org.apache.impala.authorization.ranger.RangerBufferAuditHandler.AutoFlush;
import org.apache.impala.catalog.AuthzCacheInvalidation;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
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
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
 * Operations not supported by Ranger will throw an {@link UnsupportedFeatureException}.
 */
public class RangerCatalogdAuthorizationManager implements AuthorizationManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      RangerCatalogdAuthorizationManager.class);
  private static final String AUTHZ_CACHE_INVALIDATION_MARKER = "ranger";

  private final Supplier<RangerImpalaPlugin> plugin_;
  private final CatalogServiceCatalog catalog_;

  public RangerCatalogdAuthorizationManager(Supplier<RangerImpalaPlugin> pluginSupplier,
      CatalogServiceCatalog catalog) {
    plugin_ = pluginSupplier;
    catalog_ = catalog;
  }

  @Override
  public void createRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException("CREATE ROLE is not supported by Ranger.");
  }

  @Override
  public void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException("DROP ROLE is not supported by Ranger.");
  }

  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "GRANT ROLE TO GROUP is not supported by Ranger.");
  }

  @Override
  public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "REVOKE ROLE FROM GROUP is not supported by Ranger.");
  }

  @Override
  public void grantPrivilegeToRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "GRANT <privilege> TO ROLE is not supported by Ranger.");
  }

  @Override
  public void revokePrivilegeFromRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedFeatureException(
        "REVOKE <privilege> FROM ROLE is not supported by Ranger.");
  }

  @Override
  public void grantPrivilegeToUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        header.getRequesting_user(), true, params.getPrincipal_name(),
        Collections.emptyList(), plugin_.get().getClusterName(),
        header.getClient_ip(), params.getPrivileges());

    grantPrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void revokePrivilegeFromUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        header.getRequesting_user(), false, params.getPrincipal_name(),
        Collections.emptyList(), plugin_.get().getClusterName(),
        header.getClient_ip(), params.getPrivileges());

    revokePrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void grantPrivilegeToGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        header.getRequesting_user(), true, null,
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges());

    grantPrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void revokePrivilegeFromGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        header.getRequesting_user(), false, null,
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges());

    revokePrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @VisibleForTesting
  public void grantPrivilege(List<GrantRevokeRequest> requests, String sqlStmt,
      String clientIp) throws ImpalaException {
    try {
      for (GrantRevokeRequest request : requests) {
        try (AutoFlush auditHandler = RangerBufferAuditHandler.autoFlush(sqlStmt,
            plugin_.get().getClusterName(), clientIp)) {
          plugin_.get().grantAccess(request, auditHandler);
        }
      }
    } catch (Exception e) {
      LOG.error("Error granting a privilege in Ranger: ", e);
      throw new InternalException("Error granting a privilege in Ranger. " +
          "Ranger error message: " + e.getMessage());
    }
  }

  @VisibleForTesting
  public void revokePrivilege(List<GrantRevokeRequest> requests, String sqlStmt,
      String clientIp) throws ImpalaException {
    try {
      for (GrantRevokeRequest request : requests) {
        try (AutoFlush auditHandler = RangerBufferAuditHandler.autoFlush(sqlStmt,
            plugin_.get().getClusterName(), clientIp)) {
          plugin_.get().revokeAccess(request, auditHandler);
        }
      }
    } catch (Exception e) {
      LOG.error("Error revoking a privilege in Ranger: ", e);
      throw new InternalException("Error revoking a privilege in Ranger. " +
          "Ranger error message: " + e.getMessage());
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
    // Add a single AUTHZ_CACHE_INVALIDATION catalog object called "ranger" and increment
    // its version to indicate a new cache invalidation request.
    AuthorizationDelta authzDelta = new AuthorizationDelta();
    AuthzCacheInvalidation authzCacheInvalidation =
        catalog_.incrementAuthzCacheInvalidationVersion(AUTHZ_CACHE_INVALIDATION_MARKER);
    authzDelta.addCatalogObjectAdded(authzCacheInvalidation.toTCatalogObject());
    return authzDelta;
  }

  private void refreshAuthorization(TDdlExecResponse response) {
    // Update the authorization cache invalidation marker so that the Impalads can
    // invalidate their Ranger caches. This is needed for usability reason to make sure
    // what's updated in Ranger via grant/revoke is automatically reflected to the
    // Impalad Ranger plugins.
    AuthorizationDelta authzDelta = refreshAuthorization(false);
    response.result.setUpdated_catalog_objects(authzDelta.getCatalogObjectsAdded());
  }

  public static List<GrantRevokeRequest> createGrantRevokeRequests(String grantor,
      boolean isGrant, String user, List<String> groups, String clusterName,
      String clientIp, List<TPrivilege> privileges) {
    List<GrantRevokeRequest> requests = new ArrayList<>();

    for (TPrivilege p: privileges) {
      Function<Map<String, String>, GrantRevokeRequest> createRequest = (resource) ->
          createGrantRevokeRequest(grantor, user, groups, clusterName, p.has_grant_opt,
              isGrant, p.privilege_level, resource, clientIp);

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
      List<String> groups, String clusterName, boolean withGrantOpt, boolean isGrant,
      TPrivilegeLevel level, Map<String, String> resource, String clientIp) {
    GrantRevokeRequest request = new GrantRevokeRequest();
    request.setGrantor(grantor);
    if (user != null) request.getUsers().add(user);
    if (!groups.isEmpty()) request.getGroups().addAll(groups);
    request.setDelegateAdmin(isGrant == withGrantOpt);
    request.setEnableAudit(Boolean.TRUE);
    request.setReplaceExistingPermissions(Boolean.FALSE);
    request.setClusterName(clusterName);
    request.setResource(resource);
    request.setClientIPAddress(clientIp);

    // For revoke grant option, omit the privilege
    if (!(!isGrant && withGrantOpt)) {
      if (level == TPrivilegeLevel.INSERT) {
        request.getAccessTypes().add(RangerAuthorizationChecker.UPDATE_ACCESS_TYPE);
      } else {
        request.getAccessTypes().add(level.name().toLowerCase());
      }
    }

    return request;
  }
}

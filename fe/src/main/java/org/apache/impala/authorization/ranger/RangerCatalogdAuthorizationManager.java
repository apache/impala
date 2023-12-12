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
import org.apache.impala.authorization.Privilege;
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
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
    RangerRole role = new RangerRole();
    role.setName(params.getRole_name());
    role.setCreatedByUser(requestingUser.getShortName());

    try {
      plugin_.get().createRole(role, /*resultProcessor*/ null);
    } catch (Exception e) {
      LOG.error("Error creating role {} by user {} in Ranger.", params.getRole_name(),
          requestingUser.getShortName());
      throw new InternalException("Error creating role " + params.getRole_name() +
          " by user " + requestingUser.getShortName() +
          " in Ranger. Ranger error message: " + e.getMessage());
    }
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    try {
      // We found that when a non-Ranger administrator is trying to remove a role that
      // does not exist in Ranger, the error message returned from Ranger would indicate
      // that the name of the role does not exist, which reveals the non-existence of the
      // role. This should be considered a bug of Ranger. Before the issue is resolved, we
      // always call RangerUtil#validateRangerAdmin().
      // TODO: Remove the call to validateRangerAdmin() after the bug is fixed in Ranger.
      // RANGER-3125 has been created to keep track of the issue.
      RangerUtil.validateRangerAdmin(plugin_.get(), requestingUser.getShortName());
      plugin_.get().dropRole(requestingUser.getShortName(), params.getRole_name(),
          /*resultProcessor*/ null);
    } catch (Exception e) {
      LOG.error("Error dropping role {} by user {} in Ranger.", params.getRole_name(),
          requestingUser.getShortName());
      throw new InternalException("Error dropping role " + params.getRole_name() +
          " by user " + requestingUser.getShortName() +
          " in Ranger. Ranger error message: " + e.getMessage());
    }
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Catalogd", ClassUtil.getMethodName()));
  }

  @Override
  public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    GrantRevokeRoleRequest request = createGrantRevokeRoleRequest(
        requestingUser.getShortName(), new HashSet<>(params.getRole_names()),
        new HashSet<>(params.getGroup_names()));

    try {
      // We found that granting a role to a group that is already assigned the role would
      // actually revoke the role from the group. This should be considered a bug of
      // Ranger. In this regard, as a workaround we always revoke the role from the group
      // first whether or not the role has been granted to the group. An alternative to
      // this solution is to call plugin_.get().getRolesFromUserAndGroups() to retrieve
      // the roles currently granted to the group and only call grantRole() when those
      // roles do not include the role of params.getRole_names().get(0), which is the
      // role to be granted to the group. But since there is no guarantee that the result
      // from getRolesFromUserAndGroups() is always up-to-date, we decide to call
      // revokeRole() in any case before the bug is fixed.
      // TODO: Remove the call to revokeRole() after the bug of Ranger is fixed.
      // RANGER-3126 has been created to keep track of the issue.
      plugin_.get().revokeRole(request, /*resultProcessor*/ null);
      plugin_.get().grantRole(request, /*resultProcessor*/ null);
    } catch (Exception e) {
      Pattern pattern = Pattern.compile(".*doesn't have permissions.*");
      Matcher matcher = pattern.matcher(e.getMessage());
      if (matcher.matches()) {
        // To avoid confusion, we do not use the error message from Ranger directly when
        // the grantor does not have the necessary permissions, since in the case when
        // the grantor does not have permissions to grant role, the error message from
        // Ranger would start with "User doesn't have permissions to revoke role" due to
        // the fact that we call revokeRole() first.
        // We note that we will also get this message when a Ranger administrator is
        // trying to grant a non-existing role to a group whether or not this group
        // exists.
        LOG.error("Error granting role {} to group {} by user {} in Ranger. " +
            "Ranger error message: HTTP 400 Error: User doesn't have permissions to " +
            "grant role " + params.getRole_names().get(0), params.getRole_names().get(0),
            params.getGroup_names().get(0), requestingUser.getShortName());
        throw new InternalException("Error granting role " +
            params.getRole_names().get(0) + " to group " +
            params.getGroup_names().get(0) + " by user " +
            requestingUser.getShortName() + " in Ranger. " +
            "Ranger error message: HTTP 400 Error: User doesn't have permissions to " +
            "grant role " + params.getRole_names().get(0));
      } else {
        // When a Ranger administrator tries to grant an existing role to a non-existing
        // group, we will get this error.
        LOG.error("Error granting role {} to group {} by user {} in Ranger. " +
            "Ranger error message: " + e.getMessage(), params.getRole_names().get(0),
            params.getGroup_names().get(0), requestingUser.getShortName());
        throw new InternalException("Error granting role " +
            params.getRole_names().get(0) + " to group " +
            params.getGroup_names().get(0) + " by user " +
            requestingUser.getShortName() + " in Ranger. " +
            "Ranger error message: " + e.getMessage());
      }
    }
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    GrantRevokeRoleRequest request = createGrantRevokeRoleRequest(
        requestingUser.getShortName(), new HashSet<>(params.getRole_names()),
        new HashSet<>(params.getGroup_names()));

    try {
      plugin_.get().revokeRole(request, /*resultProcessor*/ null);
    } catch (Exception e) {
      LOG.error("Error revoking role {} from group {} by user {} in Ranger. " +
          "Ranger error message: " + e.getMessage(), params.getRole_names().get(0),
          params.getGroup_names().get(0), requestingUser.getShortName());
      throw new InternalException("Error revoking role " +
          params.getRole_names().get(0) + " from group " +
          params.getGroup_names().get(0) + " by user " +
          requestingUser.getShortName() + " in Ranger. " +
          "Ranger error message: " + e.getMessage());
    }
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public void grantPrivilegeToRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ true,
        /*user*/ null, Collections.emptyList(),
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges(),
        params.getOwner_name());

    grantPrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public void revokePrivilegeFromRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ false,
        /*user*/ null, Collections.emptyList(),
        Collections.singletonList(params.getPrincipal_name()),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges(),
        params.getOwner_name());

    revokePrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    // Update the authorization refresh marker so that the Impalads can refresh their
    // Ranger caches.
    refreshAuthorization(response);
  }

  @Override
  public void grantPrivilegeToUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ true,
        params.getPrincipal_name(), Collections.emptyList(), Collections.emptyList(),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges(),
        params.getOwner_name());

    grantPrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void revokePrivilegeFromUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ false,
        params.getPrincipal_name(), Collections.emptyList(), Collections.emptyList(),
        plugin_.get().getClusterName(), header.getClient_ip(), params.getPrivileges(),
        params.getOwner_name());

    revokePrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void grantPrivilegeToGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ true,
        /*user*/ null, Collections.singletonList(params.getPrincipal_name()),
        Collections.emptyList(), plugin_.get().getClusterName(), header.getClient_ip(),
        params.getPrivileges(), params.getOwner_name());

    grantPrivilege(requests, header.getRedacted_sql_stmt(), header.getClient_ip());
    refreshAuthorization(response);
  }

  @Override
  public void revokePrivilegeFromGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    List<GrantRevokeRequest> requests = createGrantRevokeRequests(
        new User(header.getRequesting_user()).getShortName(), /*isGrant*/ false,
        /*user*/ null, Collections.singletonList(params.getPrincipal_name()),
        Collections.emptyList(), plugin_.get().getClusterName(), header.getClient_ip(),
        params.getPrivileges(), params.getOwner_name());

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
      boolean isGrant, String user, List<String> groups, List<String> roles,
      String clusterName, String clientIp, List<TPrivilege> privileges,
      String resourceOwner) {
    List<GrantRevokeRequest> requests = new ArrayList<>();

    for (TPrivilege p: privileges) {
      Function<Map<String, String>, GrantRevokeRequest> createRequest = (resource) ->
          createGrantRevokeRequest(grantor, user, groups, roles, clusterName,
              p.has_grant_opt, isGrant, p.privilege_level, resource, resourceOwner,
              clientIp);

      // Ranger Impala service definition defines 4 resources:
      // [DB -> Table -> Column]
      // [DB -> Function]
      // [URI]
      // [Storage Type -> Storage URI]
      // What it means is if we grant a particular privilege on a resource that
      // is common to other resources, we need to grant that privilege to those
      // resources.
      if (p.getColumn_name() != null || p.getTable_name() != null) {
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
      } else if (p.getUri() != null) {
        requests.add(createRequest.apply(RangerUtil.createUriResource(p)));
      } else if (p.getFn_name() != null) {
        requests.add(createRequest.apply(RangerUtil.createFunctionResource(p)));
      } else if (p.getDb_name() != null) {
        // DB is used by column and function resources.
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
        requests.add(createRequest.apply(RangerUtil.createFunctionResource(p)));
      } else if (p.getStorage_url() != null || p.getStorage_type() != null) {
        requests.add(createRequest.apply(RangerUtil.createStorageHandlerUriResource(p)));
      } else {
        // Server is used by column, function, URI, and storage handler URI resources.
        requests.add(createRequest.apply(RangerUtil.createColumnResource(p)));
        requests.add(createRequest.apply(RangerUtil.createFunctionResource(p)));
        requests.add(createRequest.apply(RangerUtil.createUriResource(p)));
        requests.add(createRequest.apply(RangerUtil.createStorageHandlerUriResource(p)));
      }
    }

    return requests;
  }

  private static GrantRevokeRequest createGrantRevokeRequest(String grantor, String user,
      List<String> groups, List<String> roles, String clusterName, boolean withGrantOpt,
      boolean isGrant, TPrivilegeLevel level, Map<String, String> resource,
      String resourceOwner, String clientIp) {
    GrantRevokeRequest request = new GrantRevokeRequest();
    request.setGrantor(grantor);
    // In a Kerberized environment, we also need to call setGrantorGroups() to provide
    // Ranger with the groups 'grantor' belongs to even though it is not required in a
    // non-Kerberized environment.
    request.setGrantorGroups(RangerUtil.getGroups(grantor));
    if (user != null) request.getUsers().add(user);
    if (!groups.isEmpty()) request.getGroups().addAll(groups);
    if (!roles.isEmpty()) request.getRoles().addAll(roles);
    request.setDelegateAdmin(isGrant && withGrantOpt);
    request.setEnableAudit(Boolean.TRUE);
    request.setReplaceExistingPermissions(Boolean.FALSE);
    request.setClusterName(clusterName);
    request.setResource(resource);
    if (resourceOwner != null) request.setOwnerUser(resourceOwner);
    request.setClientIPAddress(clientIp);

    // For revoke grant option, omit the privilege
    if (!(!isGrant && withGrantOpt)) {
      if (resource.containsKey(RangerImpalaResourceBuilder.STORAGE_TYPE)) {
        // We consider TPrivilegeLevel.ALL and TPrivilegeLevel.OWNER because for a
        // statement that grants or revokes the ALL or OWNER privileges on SERVER,
        // 'resource' could also correspond to a storage handler URI.
        // For such a statement, we add the RWSTORAGE privilege on the wildcard storage
        // handler URI. On the other hand, we won't ask Ranger to add a policy associated
        // with the RWSTORAGE privilege on the wildcard storage handler URI in a
        // query that grants or revokes a privilege other than ALL or OWNER. For
        // instance, we won't ask Ranger to add a policy granting the RWSTORAGE privilege
        // on the wildcard storage handler URI for "GRANT SELECT ON SERVER TO USER
        // non_owner".
        // Recall that no new Ranger policy will be added if we do not add a Privilege
        // to 'accessTypes' of 'request'.
        if (level == TPrivilegeLevel.ALL || level == TPrivilegeLevel.OWNER ||
            level == TPrivilegeLevel.RWSTORAGE) {
          request.getAccessTypes().add(Privilege.RWSTORAGE.name().toLowerCase());
        }
      } else {
        if (level == TPrivilegeLevel.INSERT) {
          request.getAccessTypes().add(RangerAuthorizationChecker.UPDATE_ACCESS_TYPE);
        } else if (level != TPrivilegeLevel.RWSTORAGE) {
          // When 'resource' does not correspond to a storage handler URI, we add the
          // specified 'level' as is to 'accessTypes' of 'request' only if 'level' is not
          // TPrivilegeLevel.RWSTORAGE since TPrivilegeLevel.RWSTORAGE is not
          // well-defined with respect to other types of resources, e.g., table. This
          // way we won't add a policy that grants or revokes the privilege of RWSTORAGE
          // on a 'resource' that is incompatible.
          request.getAccessTypes().add(level.name().toLowerCase());
        }
      }
    }

    return request;
  }

  /**
   * The caller of this method calls Ranger's REST API to grant/revoke roles
   * corresponding to 'targetRoleNames' to/from groups associated with 'groupNames'.
   */
  private static GrantRevokeRoleRequest createGrantRevokeRoleRequest(
      String grantor, Set<String> targetRoleNames, Set<String> groupNames) {
    GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
    request.setGrantor(grantor);
    request.setTargetRoles(targetRoleNames);
    request.setGroups(groupNames);
    // We do not set the field of 'grantOption' since WITH GRANT OPTION is not supported
    // when granting/revoking roles. By default, 'grantOption' is set to Boolean.FALSE so
    // that a user in a group assigned a role is not able to grant/revoke the role to/from
    // other groups.

    return request;
  }
}

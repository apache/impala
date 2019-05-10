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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.authorization.Authorizable;
import org.apache.impala.authorization.Authorizable.Type;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.authorization.BaseAuthorizationChecker;
import org.apache.impala.authorization.DefaultAuthorizableFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.common.InternalException;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AuthorizationChecker} that uses Ranger.
 *
 * The Ranger implementation does not use catalog to cache the authorization policy.
 * Ranger plugin uses its own cache.
 */
public class RangerAuthorizationChecker extends BaseAuthorizationChecker {
  private static final Logger LOG = LoggerFactory.getLogger(
      RangerAuthorizationChecker.class);

  // These are Ranger access types (privileges).
  public static final String UPDATE_ACCESS_TYPE = "update";
  public static final String SELECT_ACCESS_TYPE = "select";

  private final RangerImpalaPlugin plugin_;

  public RangerAuthorizationChecker(AuthorizationConfig authzConfig) {
    super(authzConfig);
    Preconditions.checkArgument(authzConfig instanceof RangerAuthorizationConfig);
    RangerAuthorizationConfig rangerConfig = (RangerAuthorizationConfig) authzConfig;
    plugin_ = new RangerImpalaPlugin(
        rangerConfig.getServiceType(), rangerConfig.getAppId());
    plugin_.init();
  }

  @Override
  protected boolean authorizeResource(AuthorizationContext authzCtx, User user,
      PrivilegeRequest request) throws InternalException {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);
    RangerAuthorizationContext rangerAuthzCtx = (RangerAuthorizationContext) authzCtx;
    List<RangerAccessResourceImpl> resources = new ArrayList<>();
    Authorizable authorizable = request.getAuthorizable();
    switch (authorizable.getType()) {
      case SERVER:
        // Hive service definition does not have a concept of server. So we define
        // server to mean all access to all resource sets.
        resources.add(new RangerImpalaResourceBuilder()
          .database("*").table("*").column("*").build());
        resources.add(new RangerImpalaResourceBuilder()
            .database("*").function("*").build());
        resources.add(new RangerImpalaResourceBuilder().uri("*").build());
        break;
      case DB:
        resources.add(new RangerImpalaResourceBuilder()
            .database(authorizable.getDbName())
            .build());
        break;
      case TABLE:
        resources.add(new RangerImpalaResourceBuilder()
            .database(authorizable.getDbName())
            .table(authorizable.getTableName())
            .build());
        break;
      case COLUMN:
        RangerImpalaResourceBuilder builder = new RangerImpalaResourceBuilder();
        builder.database(authorizable.getDbName());
        // * in Ranger means "all". For example to check access for all columns, we need
        // to create a request, such as:
        // [server=server1, database=foo, table=bar, column=*]
        //
        // "Any" column access is special in Ranger. For example if we want to check if
        // we have access to "any" column on a particular table, we need to build a
        // request without the column defined in the resource and use a special
        // ANY_ACCESS access type.
        //
        // For example any column in foo.bar table:
        // access type: RangerPolicyEngine.ANY_ACCESS
        // resources: [server=server1, database=foo, table=bar]
        if (request.getPrivilege() != Privilege.ANY ||
          !DefaultAuthorizableFactory.ALL.equals(authorizable.getTableName())) {
          builder.table(authorizable.getTableName());
        }
        if (request.getPrivilege() != Privilege.ANY ||
          !DefaultAuthorizableFactory.ALL.equals(authorizable.getColumnName())) {
          builder.column(authorizable.getColumnName());
        }
        resources.add(builder.build());
        break;
      case FUNCTION:
        resources.add(new RangerImpalaResourceBuilder()
            .database(authorizable.getDbName())
            .function(authorizable.getFnName())
            .build());
        break;
      case URI:
        resources.add(new RangerImpalaResourceBuilder()
            .uri(authorizable.getName())
            .build());
        break;
      default:
        throw new IllegalArgumentException(String.format("Invalid authorizable type: %s",
            authorizable.getType()));
    }

    for (RangerAccessResourceImpl resource: resources) {
      if (request.getPrivilege() == Privilege.ANY) {
        if (!authorizeResource(rangerAuthzCtx, resource, user, request.getPrivilege())) {
          return false;
        }
      } else {
        boolean authorized = request.getPrivilege().hasAnyOf() ?
            authorizeAny(rangerAuthzCtx, resource, user,
                request.getPrivilege().getImpliedPrivileges()) :
            authorizeAll(rangerAuthzCtx, resource, user,
                request.getPrivilege().getImpliedPrivileges());
        if (!authorized) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void postAuthorize(AuthorizationContext authzCtx) {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    super.postAuthorize(authzCtx);
    RangerBufferAuditHandler auditHandler =
        ((RangerAuthorizationContext) authzCtx).getAuditHandler();
    auditHandler.flush();
  }

  @Override
  protected void authorizeRowFilterAndColumnMask(User user,
      List<PrivilegeRequest> privilegeRequests)
      throws AuthorizationException, InternalException {
    for (PrivilegeRequest request : privilegeRequests) {
      if (request.getAuthorizable().getType() == Type.COLUMN) {
        authorizeColumnMask(user,
            request.getAuthorizable().getDbName(),
            request.getAuthorizable().getTableName(),
            request.getAuthorizable().getColumnName());
      } else if (request.getAuthorizable().getType() == Type.TABLE) {
        if (request.getAuthorizable().getType() == Type.TABLE) {
          authorizeRowFilter(user,
              request.getAuthorizable().getDbName(),
              request.getAuthorizable().getTableName());
        }
      }
    }
  }

  @Override
  public void invalidateAuthorizationCache() {
    long startTime = System.currentTimeMillis();
    try {
      plugin_.refreshPoliciesAndTags();
    } finally {
      LOG.debug("Refreshing Ranger policies took {} ms",
          (System.currentTimeMillis() - startTime));
    }
  }

  @Override
  public AuthorizationContext createAuthorizationContext(boolean doAudits) {
    RangerAuthorizationContext authzCtx = new RangerAuthorizationContext();
    if (doAudits) {
      // Any statement that goes through {@link authorize} will need to have audit logs.
      authzCtx.setAuditHandler(new RangerBufferAuditHandler());
    }
    return authzCtx;
  }

  @Override
  protected void authorizeTableAccess(AuthorizationContext authzCtx,
      AnalysisResult analysisResult, FeCatalog catalog, List<PrivilegeRequest> requests)
      throws AuthorizationException, InternalException {
    RangerAuthorizationContext originalCtx = (RangerAuthorizationContext) authzCtx;
    // case 1: table (select) OK --> add the table event
    // case 2: table (non-select) ERROR --> add the table event
    // case 3: table (select) ERROR, columns (select) OK -> only add the column events
    // case 4: table (select) ERROR, columns (select) ERROR --> only add the first column
    //                                                          event
    RangerAuthorizationContext tmpCtx = new RangerAuthorizationContext();
    tmpCtx.setAuditHandler(new RangerBufferAuditHandler());
    try {
      super.authorizeTableAccess(tmpCtx, analysisResult, catalog, requests);
    } catch (AuthorizationException e) {
      tmpCtx.getAuditHandler().getAuthzEvents().stream()
          .filter(evt ->
              // case 2: get the first failing non-select table
              (!"select".equals(evt.getAccessType()) &&
                  "@table".equals(evt.getResourceType())) ||
              // case 4: get the first failing column
              ("@column".equals(evt.getResourceType()) && evt.getAccessResult() == 0))
          .findFirst()
          .ifPresent(evt -> originalCtx.getAuditHandler().getAuthzEvents().add(evt));
      throw e;
    } finally {
      // case 1 & 4: we only add the successful events. The first table-level access
      // check is only for the short-circuit, we don't want to add an event for that.
      List<AuthzAuditEvent> events = tmpCtx.getAuditHandler().getAuthzEvents().stream()
          .filter(evt -> evt.getAccessResult() != 0)
          .collect(Collectors.toList());
      originalCtx.getAuditHandler().getAuthzEvents().addAll(events);
    }
  }

  /**
   * This method checks if column mask is enabled on the given columns and deny access
   * when column mask is enabled by throwing an {@link AuthorizationException}. This is
   * to prevent data leak when Hive has column mask enabled but not in Impala.
   */
  private void authorizeColumnMask(User user, String dbName, String tableName,
      String columnName) throws InternalException, AuthorizationException {
    RangerAccessResourceImpl resource = new RangerImpalaResourceBuilder()
        .database(dbName)
        .table(tableName)
        .column(columnName)
        .build();
    RangerAccessRequest req = new RangerAccessRequestImpl(resource,
        SELECT_ACCESS_TYPE, user.getShortName(), getUserGroups(user));
    if (plugin_.evalDataMaskPolicies(req, null).isMaskEnabled()) {
      throw new AuthorizationException(String.format(
          "Impala does not support column masking yet. Column masking is enabled on " +
              "column: %s.%s.%s", dbName, tableName, columnName));
    }
  }

  /**
   * This method checks if row filter is enabled on the given tables and deny access
   * when row filter is enabled by throwing an {@link AuthorizationException} . This is
   * to prevent data leak when Hive has row filter enabled but not in Impala.
   */
  private void authorizeRowFilter(User user, String dbName, String tableName)
      throws InternalException, AuthorizationException {
    RangerAccessResourceImpl resource = new RangerImpalaResourceBuilder()
        .database(dbName)
        .table(tableName)
        .build();
    RangerAccessRequest req = new RangerAccessRequestImpl(resource,
        SELECT_ACCESS_TYPE, user.getShortName(), getUserGroups(user));
    if (plugin_.evalRowFilterPolicies(req, null).isRowFilterEnabled()) {
      throw new AuthorizationException(String.format(
          "Impala does not support row filtering yet. Row filtering is enabled " +
              "on table: %s.%s", dbName, tableName));
    }
  }

  @Override
  public Set<String> getUserGroups(User user) throws InternalException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user.getShortName());
    return new HashSet<>(ugi.getGroups());
  }

  private boolean authorizeAny(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, EnumSet<Privilege> privileges)
      throws InternalException {
    for (Privilege privilege: privileges) {
      if (authorizeResource(authzCtx, resource, user, privilege)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeAll(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, EnumSet<Privilege> privileges)
      throws InternalException {
    for (Privilege privilege: privileges) {
      if (!authorizeResource(authzCtx, resource, user, privilege)) {
        return false;
      }
    }
    return true;
  }

  private boolean authorizeResource(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, Privilege privilege)
      throws InternalException {
    String accessType;
    if (privilege == Privilege.ANY) {
      accessType = RangerPolicyEngine.ANY_ACCESS;
    } else if (privilege == Privilege.INSERT) {
      // Ranger plugin for Hive considers INSERT to be UPDATE.
      accessType = UPDATE_ACCESS_TYPE;
    } else {
      accessType = privilege.name().toLowerCase();
    }
    RangerAccessRequest request = new RangerAccessRequestImpl(resource,
        accessType, user.getShortName(), getUserGroups(user));
    RangerAccessResult result = plugin_.isAccessAllowed(request,
        authzCtx.getAuditHandler());
    return result != null && result.getIsAllowed();
  }

  @VisibleForTesting
  public RangerImpalaPlugin getRangerImpalaPlugin() { return plugin_; }
}

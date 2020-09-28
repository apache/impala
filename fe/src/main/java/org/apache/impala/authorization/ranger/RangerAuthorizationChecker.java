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
import org.apache.commons.lang.StringUtils;
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
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.util.EventSequence;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
    plugin_ = RangerImpalaPlugin.getInstance(rangerConfig.getServiceType(),
        rangerConfig.getAppId());
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
            .owner(authorizable.getOwnerUser())
            .build());
        break;
      case TABLE:
        resources.add(new RangerImpalaResourceBuilder()
            .database(authorizable.getDbName())
            .table(authorizable.getTableName())
            .owner(authorizable.getOwnerUser())
            .build());
        break;
      case COLUMN:
        RangerImpalaResourceBuilder builder = new RangerImpalaResourceBuilder();
        builder.database(authorizable.getDbName());
        // * in Ranger means "all". For example, to check access for all columns, we need
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
        builder.owner(authorizable.getOwnerUser());
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
        if (!authorizeResource(rangerAuthzCtx, resource, user, request.getPrivilege(),
            ((RangerAuthorizationContext) authzCtx).getAuditHandler())) {
          return false;
        }
      } else {
        boolean authorized = request.getPrivilege().hasAnyOf() ?
            authorizeAny(rangerAuthzCtx, resource, user, request.getPrivilege()) :
            authorizeAll(rangerAuthzCtx, resource, user, request.getPrivilege());
        if (!authorized) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void postAuthorize(AuthorizationContext authzCtx, boolean authzOk) {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    super.postAuthorize(authzCtx, authzOk);
    // Apply the deduplicated column masking events to update the List of all
    // AuthzAuditEvent's only if the authorization is successful.
    if (authzOk) {
      ((RangerAuthorizationContext) authzCtx).applyDeduplicatedAuthzEvents();
    }
    RangerBufferAuditHandler auditHandler =
        ((RangerAuthorizationContext) authzCtx).getAuditHandler();
    auditHandler.flush();
  }

  @Override
  protected void authorizeRowFilterAndColumnMask(User user,
      List<PrivilegeRequest> privilegeRequests)
      throws AuthorizationException, InternalException {
    boolean isColumnMaskingEnabled = BackendConfig.INSTANCE.isColumnMaskingEnabled();
    for (PrivilegeRequest request : privilegeRequests) {
      if (!isColumnMaskingEnabled
          && request.getAuthorizable().getType() == Type.COLUMN) {
        authorizeColumnMask(user,
            request.getAuthorizable().getDbName(),
            request.getAuthorizable().getTableName(),
            request.getAuthorizable().getColumnName());
      } else if (request.getAuthorizable().getType() == Type.TABLE) {
        authorizeRowFilter(user,
            request.getAuthorizable().getDbName(),
            request.getAuthorizable().getTableName());
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
  public AuthorizationContext createAuthorizationContext(boolean doAudits,
      String sqlStmt, TSessionState sessionState, Optional<EventSequence> timeline) {
    RangerAuthorizationContext authzCtx =
        new RangerAuthorizationContext(sessionState, timeline);
    if (doAudits) {
      // Any statement that goes through {@link authorize} will need to have audit logs.
      if (sqlStmt != null) {
        authzCtx.setAuditHandler(new RangerBufferAuditHandler(
            sqlStmt, plugin_.getClusterName(),
            sessionState.getNetwork_address().getHostname()));
      } else {
        authzCtx.setAuditHandler(new RangerBufferAuditHandler());
      }
    }
    return authzCtx;
  }

  @Override
  protected void authorizeTableAccess(AuthorizationContext authzCtx,
      AnalysisResult analysisResult, FeCatalog catalog, List<PrivilegeRequest> requests)
      throws AuthorizationException, InternalException {
    RangerAuthorizationContext originalCtx = (RangerAuthorizationContext) authzCtx;
    RangerBufferAuditHandler originalAuditHandler = originalCtx.getAuditHandler();
    // case 1: table (select) OK --> add the table event
    // case 2: table (non-select) ERROR --> add the table event
    // case 3: table (select) ERROR, columns (select) OK -> only add the column events
    // case 4: table (select) ERROR, columns (select) ERROR --> only add the first column
    //                                                          event
    RangerAuthorizationContext tmpCtx = new RangerAuthorizationContext(
        originalCtx.getSessionState(), originalCtx.getTimeline());
    tmpCtx.setAuditHandler(new RangerBufferAuditHandler(originalAuditHandler));
    try {
      super.authorizeTableAccess(tmpCtx, analysisResult, catalog, requests);
    } catch (AuthorizationException e) {
      tmpCtx.getAuditHandler().getAuthzEvents().stream()
          .filter(evt ->
              // case 2: get the first failing non-select table
              (!"select".equalsIgnoreCase(evt.getAccessType()) &&
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
    if (evalColumnMask(user, dbName, tableName, columnName, null).isMaskEnabled()) {
      throw new AuthorizationException(String.format(
          "Column masking is disabled by --enable_column_masking flag. Can't access " +
              "column %s.%s.%s that has column masking policy.",
          dbName, tableName, columnName));
    }
  }

  @Override
  public boolean needsMaskingOrFiltering(User user, String dbName, String tableName,
      List<String> requiredColumns) throws InternalException {
    for (String column: requiredColumns) {
      if (evalColumnMask(user, dbName, tableName, column, null)
          .isMaskEnabled()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String createColumnMask(User user, String dbName, String tableName,
      String columnName, AuthorizationContext rangerCtx) throws InternalException {
    RangerBufferAuditHandler auditHandler =
        ((RangerAuthorizationContext) rangerCtx).getAuditHandler();
    int numAuthzAuditEventsBefore = auditHandler.getAuthzEvents().size();
    RangerAccessResult accessResult = evalColumnMask(user, dbName, tableName, columnName,
        auditHandler);
    // When a user adds an "Unmasked" policy for 'columnName' that retains the original
    // value, accessResult.getMaskType() would be "MASK_NONE". We do not need to log such
    // an event. Removing such an event also makes the logged audits consistent when
    // there is at least one "Unmasked" policy. Recall that this method is the only place
    // evalColumnMask() is called with a non-null RangerBufferAuditHandler to produce
    // AuthzAuditEvent's. When an "Unmasked" policy is the only column masking policy
    // involved in this query, this method would not be invoked because isMaskEnabled()
    // of the corresponding RangerAccessResult is false in needsMaskingOrFiltering(),
    // whereas this method will be called when there are policies of other types. If we
    // do not remove the event of the "Unmasked" policy, we will have one more logged
    // event in the latter case where there are policies other than "Unmasked".

    // Moreover, notice that there will be an AuthzAuditEvent added to
    // 'auditHandler.getAuthzEvents()' as long as there exists a column masking policy
    // associated with 'dbName', 'tableName', and 'columnName', even though the policy
    // does NOT apply to 'user'. In this case, accessResult.isMaskEnabled() would be
    // false, and accessResult.getPolicyId() would be -1. In addition, the field of
    // 'accessResult' would be 0 for that AuthzAuditEvent. An AuthzAuditEvent with
    // 'accessResult' equal to 0 will be considered as an indication of a failed access
    // request in RangerBufferAuditHandler#flush() even though there is no failed access
    // request at all for the current query and thus we need to filter out this
    // AuthzAuditEvent. If we did not filter it out, other AuthzAuditEvent's
    // corresponding to successful/allowed access requests in the same query will not be
    // logged since only the first AuthzAuditEvent with 'accessResult' equal to 0 is
    // logged in RangerBufferAuditHandler#flush().
    List<AuthzAuditEvent> auditEvents = auditHandler.getAuthzEvents();
    Preconditions.checkState(auditEvents.size() - numAuthzAuditEventsBefore <= 1);
    if (auditEvents.size() > numAuthzAuditEventsBefore &&
        !accessResult.isMaskEnabled()) {
      // Recall that the same instance of RangerAuthorizationContext is passed to
      // createColumnMask() every time this method is called. Thus the same instance of
      // RangerBufferAuditHandler is provided for evalColumnMask() to log the event.
      // Since there will be exactly one event added to 'auditEvents' when there is a
      // corresponding column masking policy, i.e., accessResult.isMaskEnabled()
      // evaluates to true, we only need to process the last event on 'auditEvents'.
      auditHandler.getAuthzEvents().remove(auditEvents.size() - 1);
    }
    // No column masking policies, return the original column.
    if (!accessResult.isMaskEnabled()) return columnName;
    String maskType = accessResult.getMaskType();
    RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = accessResult.getMaskTypeDef();
    Preconditions.checkNotNull(maskType);
    Preconditions.checkState(!auditEvents.isEmpty());
    auditEvents.get(auditEvents.size() - 1).setAccessType(maskType.toLowerCase());
    // The expression used to replace the original column.
    String maskedColumn = columnName;
    // The expression of the mask type. Column names are referenced by "{col}".
    // Transformer examples for the builtin mask types:
    //   mask type           transformer
    //   MASK                mask({col})
    //   MASK_SHOW_LAST_4    mask_show_last_n({col}, 4, 'x', 'x', 'x', -1, '1')
    //   MASK_SHOW_FIRST_4   mask_show_first_n({col}, 4, 'x', 'x', 'x', -1, '1')
    //   MASK_HASH           mask_hash({col})
    //   MASK_DATE_SHOW_YEAR mask({col}, 'x', 'x', 'x', -1, '1', 1, 0, -1)
    String transformer = null;
    if (maskTypeDef != null) {
      transformer = maskTypeDef.getTransformer();
    }
    if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
      maskedColumn = "NULL";
    } else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
      String maskedValue = accessResult.getMaskedValue();
      if (maskedValue == null) {
        maskedColumn = "NULL";
      } else {
        maskedColumn = maskedValue.replace("{col}", columnName);
      }
    } else if (StringUtils.isNotEmpty(transformer)) {
      maskedColumn = transformer.replace("{col}", columnName);
    }
    LOG.info("dbName: {}, tableName: {}, column: {}, maskType: {}, columnTransformer: {}",
        dbName, tableName, columnName, maskType, maskedColumn);
    return maskedColumn;
  }

  @Override
  public void postAnalyze(AuthorizationContext authzCtx) {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    ((RangerAuthorizationContext) authzCtx).stashAuditEvents(plugin_);
  }

  /**
   * Evaluate column masking policies on the given column and returns the result.
   * A RangerAccessResult contains the matched policy details and the masked column.
   * Note that Ranger will add an AuthzAuditEvent to auditHandler.getAuthzEvents() as
   * long as there exists a policy for the given column even though the policy does not
   * apply to 'user'.
   */
  private RangerAccessResult evalColumnMask(User user, String dbName,
      String tableName, String columnName, RangerBufferAuditHandler auditHandler)
      throws InternalException {
    RangerAccessResourceImpl resource = new RangerImpalaResourceBuilder()
        .database(dbName)
        .table(tableName)
        .column(columnName)
        .build();
    RangerAccessRequest req = new RangerAccessRequestImpl(resource,
        SELECT_ACCESS_TYPE, user.getShortName(), getUserGroups(user));
    // The method evalDataMaskPolicies() only checks whether there is a corresponding
    // column masking policy on the Ranger server and thus does not check whether the
    // requesting user/group is granted the necessary privilege on the specified
    // resource. No AnalysisException or AuthorizationException will be thrown if the
    // requesting user/group does not possess the necessary privilege.
    return plugin_.evalDataMaskPolicies(req, auditHandler);
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
    UserGroupInformation ugi;
    if (RuntimeEnv.INSTANCE.isTestEnv() ||
        BackendConfig.INSTANCE.useCustomizedUserGroupsMapperForRanger()) {
      ugi = UserGroupInformation.createUserForTesting(user.getShortName(),
          new String[]{user.getShortName()});
    } else {
      ugi = UserGroupInformation.createRemoteUser(user.getShortName());
    }
    return new HashSet<>(ugi.getGroups());
  }

  private boolean authorizeAny(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, Privilege privilege)
      throws InternalException {
    boolean authorized = false;
    RangerBufferAuditHandler originalAuditHandler = authzCtx.getAuditHandler();
    // Use a temporary audit handler instead of the original audit handler
    // so that we know all the audit events generated by the temporary audit
    // handler are contained to the given resource.
    RangerBufferAuditHandler tmpAuditHandler = originalAuditHandler == null ?
        null : new RangerBufferAuditHandler(originalAuditHandler);
    for (Privilege impliedPrivilege: privilege.getImpliedPrivileges()) {
      if (authorizeResource(authzCtx, resource, user, impliedPrivilege,
          tmpAuditHandler)) {
        authorized = true;
        break;
      }
    }
    if (originalAuditHandler != null) {
      updateAuditEvents(tmpAuditHandler, originalAuditHandler, true /*any*/,
          privilege);
    }
    return authorized;
  }

  private boolean authorizeAll(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, Privilege privilege)
      throws InternalException {
    boolean authorized = true;
    RangerBufferAuditHandler originalAuditHandler = authzCtx.getAuditHandler();
    // Use a temporary audit handler instead of the original audit handler
    // so that we know all the audit events generated by the temporary audit
    // handler are contained to the given resource.
    RangerBufferAuditHandler tmpAuditHandler = originalAuditHandler == null ?
        null : new RangerBufferAuditHandler(originalAuditHandler);
    for (Privilege impliedPrivilege: privilege.getImpliedPrivileges()) {
      if (!authorizeResource(authzCtx, resource, user, impliedPrivilege,
          tmpAuditHandler)) {
        authorized = false;
        break;
      }
    }
    if (originalAuditHandler != null) {
      updateAuditEvents(tmpAuditHandler, originalAuditHandler, false /*not any*/,
          privilege);
    }
    return authorized;
  }

  /**
   * Updates the {@link AuthzAuditEvent} from the source to the destination
   * {@link RangerBufferAuditHandler} with the given privilege.
   *
   * For example we want to log:
   * VIEW_METADATA - Allowed
   * vs
   * INSERT - Denied, INSERT - Allowed.
   */
  private static void updateAuditEvents(RangerBufferAuditHandler source,
      RangerBufferAuditHandler dest, boolean isAny, Privilege privilege) {
    // A custom audit event to log the actual privilege instead of the implied
    // privileges.
    AuthzAuditEvent newAuditEvent = null;
    for (AuthzAuditEvent auditEvent : source.getAuthzEvents()) {
      if (auditEvent.getAccessResult() == (isAny ? 1 : 0)) {
        newAuditEvent = auditEvent;
        break; // break if at least one is a success.
      } else {
        newAuditEvent = auditEvent;
      }
    }
    if (newAuditEvent != null) {
      newAuditEvent.setAccessType(privilege.name().toLowerCase());
      dest.getAuthzEvents().add(newAuditEvent);
    }
  }

  private boolean authorizeResource(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, User user, Privilege privilege,
      RangerBufferAuditHandler auditHandler) throws InternalException {
    String accessType;
    if (privilege == Privilege.ANY) {
      accessType = RangerPolicyEngine.ANY_ACCESS;
    } else if (privilege == Privilege.INSERT) {
      // Ranger plugin for Hive considers INSERT to be UPDATE.
      accessType = UPDATE_ACCESS_TYPE;
    } else {
      accessType = privilege.name().toLowerCase();
    }
    RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource,
        accessType, user.getShortName(), getUserGroups(user));
    request.setClusterName(plugin_.getClusterName());
    if (authzCtx.getSessionState() != null) {
      request.setClientIPAddress(
          authzCtx.getSessionState().getNetwork_address().getHostname());
    }
    RangerAccessResult authorized = plugin_.isAccessAllowed(request, auditHandler);
    return authorized != null && authorized.getIsAllowed();
  }

  @VisibleForTesting
  public RangerImpalaPlugin getRangerImpalaPlugin() { return plugin_; }
}

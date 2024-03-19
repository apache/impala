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
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.authorization.Authorizable;
import org.apache.impala.authorization.Authorizable.Type;
import org.apache.impala.authorization.AuthorizableTable;
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
    Privilege privilege = request.getPrivilege();
    switch (authorizable.getType()) {
      case SERVER:
        // Hive service definition does not have a concept of server. So we define
        // server to mean all access to all resource sets.
        resources.add(new RangerImpalaResourceBuilder()
          .database("*").table("*").column("*").build());
        resources.add(new RangerImpalaResourceBuilder()
            .database("*").function("*").build());
        resources.add(new RangerImpalaResourceBuilder().uri("*").build());
        if (privilege == Privilege.ALL || privilege == Privilege.OWNER ||
            privilege == Privilege.RWSTORAGE) {
          resources.add(new RangerImpalaResourceBuilder()
              .storageType("*").storageUri("*").build());
        }
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
        if (privilege != Privilege.ANY ||
          !DefaultAuthorizableFactory.ALL.equals(authorizable.getTableName())) {
          builder.table(authorizable.getTableName());
        }
        if (privilege != Privilege.ANY ||
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
      case STORAGEHANDLER_URI:
        resources.add(new RangerImpalaResourceBuilder()
            .storageType(authorizable.getStorageType())
            .storageUri(authorizable.getStorageUri())
            .build());
        break;
      default:
        throw new IllegalArgumentException(String.format("Invalid authorizable type: %s",
            authorizable.getType()));
    }

    for (RangerAccessResourceImpl resource: resources) {
      if (privilege == Privilege.ANY) {
        if (!authorizeResource(rangerAuthzCtx, user, resource, authorizable, privilege,
            rangerAuthzCtx.getAuditHandler())) {
          return false;
        }
      } else {
        boolean authorized = privilege.hasAnyOf() ?
            authorizeAny(rangerAuthzCtx, resource, authorizable, user, privilege) :
            authorizeAll(rangerAuthzCtx, resource, authorizable, user, privilege);
        if (!authorized) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void postAuthorize(AuthorizationContext authzCtx, boolean authzOk,
      boolean analysisOk) {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    super.postAuthorize(authzCtx, authzOk, analysisOk);
    // Consolidate the audit log entries and apply the deduplicated column masking events
    // to update the List of all AuthzAuditEvent's only if the authorization is
    // successful.
    if (authzOk) {
      // We consolidate the audit log entries before invoking
      // applyDeduplicatedAuthzEvents() where we add the deduplicated table
      // masking-related log entries to 'auditHandler' because currently a Ranger column
      // masking policy can be applied to only one single column in a table. Thus, we do
      // not need to combine log entries produced due to column masking policies.
      ((RangerAuthorizationContext) authzCtx).consolidateAuthzEvents();
      ((RangerAuthorizationContext) authzCtx).applyDeduplicatedAuthzEvents();
    }
    RangerBufferAuditHandler auditHandler =
        ((RangerAuthorizationContext) authzCtx).getAuditHandler();
    if (authzOk && !analysisOk) {
      // When the query was authorized, we do not send any audit log entry to the Ranger
      // server if there was an AnalysisException during query analysis.
      // We still have to call clear() to remove audit log entries in this case because
      // the current test framework checks the contents in auditHandler.getAuthzEvents()
      // to determine whether the correct audit events are collected.
      auditHandler.getAuthzEvents().clear();
    } else {
      // We send audit log entries to the Ranger server only if authorization failed or
      // analysis succeeded.
      auditHandler.flush();
    }
  }

  @Override
  protected void authorizeRowFilterAndColumnMask(User user,
      List<PrivilegeRequest> privilegeRequests)
      throws AuthorizationException, InternalException {
    boolean isColumnMaskingEnabled = BackendConfig.INSTANCE.isColumnMaskingEnabled();
    boolean isRowFilteringEnabled = BackendConfig.INSTANCE.isRowFilteringEnabled();
    if (isColumnMaskingEnabled && isRowFilteringEnabled) return;
    for (PrivilegeRequest request : privilegeRequests) {
      if (!isColumnMaskingEnabled
          && request.getAuthorizable().getType() == Type.COLUMN) {
        throwIfColumnMaskingRequired(user,
            request.getAuthorizable().getDbName(),
            request.getAuthorizable().getTableName(),
            request.getAuthorizable().getColumnName());
      } else if (!isRowFilteringEnabled
          && request.getAuthorizable().getType() == Type.TABLE) {
        throwIfRowFilteringRequired(user,
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
    // case 1: table (select) OK, columns (select) OK --> add the table event,
    //                                                    add the column events
    // case 2: table (non-select) ERROR --> add the table event
    // case 3: table (select) ERROR, columns (select) OK -> only add the column events
    // case 4: table (select) ERROR, columns (select) ERROR --> add the table event
    // case 5: table (select) ERROR --> add the table event
    //         This could happen when the select request for a non-existing table fails
    //         the authorization.
    // case 6: table (select) OK, columns (select) ERROR --> add the first column event
    //         This could happen when the requesting user is granted the select privilege
    //         on the table but is denied access to a column in the same table in Ranger.
    RangerAuthorizationContext tmpCtx = new RangerAuthorizationContext(
        originalCtx.getSessionState(), originalCtx.getTimeline());
    tmpCtx.setAuditHandler(new RangerBufferAuditHandler(originalAuditHandler));
    AuthorizationException authorizationException = null;
    try {
      super.authorizeTableAccess(tmpCtx, analysisResult, catalog, requests);
    } catch (AuthorizationException e) {
      authorizationException = e;
      tmpCtx.getAuditHandler().getAuthzEvents().stream()
          .filter(evt ->
              // case 2: get the first failing non-select table
              (!"select".equalsIgnoreCase(evt.getAccessType()) &&
                  "@table".equals(evt.getResourceType())) ||
              // case 4 & 5 & 6: get the table or a column event
              (("@table".equals(evt.getResourceType()) ||
                  "@column".equals(evt.getResourceType())) &&
                  evt.getAccessResult() == 0))
          .findFirst()
          .ifPresent(evt -> originalCtx.getAuditHandler().getAuthzEvents().add(evt));
      throw e;
    } finally {
      // We should not add successful events when there was an AuthorizationException.
      // Specifically, we should not add the successful table event in case 6.
      if (authorizationException == null) {
        // case 1 & 3: we only add the successful events.
        // TODO(IMPALA-11381): Consider whether we should keep the table-level event which
        // corresponds to a check only for short-circuiting authorization.
        List<AuthzAuditEvent> events = tmpCtx.getAuditHandler().getAuthzEvents().stream()
            .filter(evt -> evt.getAccessResult() != 0)
            .collect(Collectors.toList());
        originalCtx.getAuditHandler().getAuthzEvents().addAll(events);
      }
    }
  }

  /**
   * This method throws an {@link AuthorizationException} if column mask is enabled on the
   * given column. This is used to prevent data leak when Hive has column mask enabled but
   * it's disabled in Impala.
   */
  private void throwIfColumnMaskingRequired(User user, String dbName, String tableName,
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
    return needsRowFiltering(user, dbName, tableName);
  }

  @Override
  public boolean needsRowFiltering(User user, String dbName, String tableName)
      throws InternalException {
    return evalRowFilter(user, dbName, tableName, null).isRowFilterEnabled();
  }

  /**
   * Util method for removing stale audit events of column masking and row filtering
   * policies. See comments below for cases we need this.
   *
   * TODO: Revisit RangerBufferAuditHandler and compare it to
   *  org.apache.ranger.authorization.hive.authorizer.RangerHiveAuditHandler to see
   *  whether we can avoid generating stale audit events there.
   * TODO: Do we really need this for row-filtering?
   */
  private void removeStaleAudits(RangerBufferAuditHandler auditHandler,
      int numAuthzAuditEventsBefore) {
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
    if (auditEvents.size() > numAuthzAuditEventsBefore) {
      // Recall that the same instance of RangerAuthorizationContext is passed to
      // createColumnMask() every time this method is called. Thus the same instance of
      // RangerBufferAuditHandler is provided for evalColumnMask() to log the event.
      // Since there will be exactly one event added to 'auditEvents' when there is a
      // corresponding column masking policy, i.e., accessResult.isMaskEnabled()
      // evaluates to true, we only need to process the last event on 'auditEvents'.
      auditHandler.getAuthzEvents().remove(auditEvents.size() - 1);
    }
  }

  @Override
  public String createColumnMask(User user, String dbName, String tableName,
      String columnName, AuthorizationContext rangerCtx) throws InternalException {
    RangerBufferAuditHandler auditHandler = (rangerCtx == null) ? null:
        ((RangerAuthorizationContext) rangerCtx).getAuditHandler();
    int numAuthzAuditEventsBefore = (auditHandler == null) ? 0 :
        auditHandler.getAuthzEvents().size();
    RangerAccessResult accessResult = evalColumnMask(user, dbName, tableName, columnName,
        auditHandler);
    if (!accessResult.isMaskEnabled() && auditHandler != null) {
      // No column masking policies, remove any possible stale audit events and
      // return the original column.
      removeStaleAudits(auditHandler, numAuthzAuditEventsBefore);
      return columnName;
    }
    String maskType = accessResult.getMaskType();
    RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = accessResult.getMaskTypeDef();
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
    LOG.trace(
        "dbName: {}, tableName: {}, column: {}, maskType: {}, columnTransformer: {}",
        dbName, tableName, columnName, maskType, maskedColumn);
    return maskedColumn;
  }

  @Override
  public String createRowFilter(User user, String dbName, String tableName,
      AuthorizationContext rangerCtx) throws InternalException {
    RangerBufferAuditHandler auditHandler = (rangerCtx == null) ? null :
        ((RangerAuthorizationContext) rangerCtx).getAuditHandler();
    int numAuthzAuditEventsBefore = (auditHandler == null) ? 0 :
        auditHandler.getAuthzEvents().size();
    RangerAccessResult accessResult = evalRowFilter(user, dbName, tableName,
        auditHandler);
    if (!accessResult.isRowFilterEnabled() && auditHandler != null) {
      // No row filtering policies, remove any possible stale audit events.
      removeStaleAudits(auditHandler, numAuthzAuditEventsBefore);
      return null;
    }
    String filter = accessResult.getFilterExpr();
    LOG.trace("dbName: {}, tableName: {}, rowFilter: {}", dbName, tableName, filter);
    return filter;
  }

  @Override
  public void postAnalyze(AuthorizationContext authzCtx) {
    Preconditions.checkArgument(authzCtx instanceof RangerAuthorizationContext);
    ((RangerAuthorizationContext) authzCtx).stashTableMaskingAuditEvents(plugin_);
  }

  /**
   * Evaluate column masking policies on the given column and returns the result,
   * a RangerAccessResult contains the matched policy details and the masked column.
   * Note that Ranger will add an AuthzAuditEvent to auditHandler.getAuthzEvents() as
   * long as there exists a policy for the given column even though the policy does not
   * apply to 'user'.
   */
  private RangerAccessResult evalColumnMask(User user, String dbName,
      String tableName, String columnName, RangerBufferAuditHandler auditHandler)
      throws InternalException {
    Preconditions.checkNotNull(user);
    RangerAccessResourceImpl resource = new RangerImpalaResourceBuilder()
        .database(dbName)
        .table(tableName)
        .column(columnName)
        .build();
    RangerAccessRequestImpl req = new RangerAccessRequestImpl();
    req.setResource(resource);
    req.setAccessType(SELECT_ACCESS_TYPE);
    req.setUser(user.getShortName());
    req.setUserGroups(getUserGroups(user));
    // The method evalDataMaskPolicies() only checks whether there is a corresponding
    // column masking policy on the Ranger server and thus does not check whether the
    // requesting user/group is granted the necessary privilege on the specified
    // resource. No AnalysisException or AuthorizationException will be thrown if the
    // requesting user/group does not possess the necessary privilege.
    return plugin_.evalDataMaskPolicies(req, auditHandler);
  }

  /**
   * Evaluate row filtering policies on the given table and returns the result,
   * a RangerAccessResult contains the matched policy details and the filter string.
   */
  private RangerAccessResult evalRowFilter(User user, String dbName, String tableName,
      RangerBufferAuditHandler auditHandler) throws InternalException {
    Preconditions.checkNotNull(user);
    RangerAccessResourceImpl resource = new RangerImpalaResourceBuilder()
        .database(dbName)
        .table(tableName)
        .build();
    RangerAccessRequestImpl req = new RangerAccessRequestImpl();
    req.setResource(resource);
    req.setAccessType(SELECT_ACCESS_TYPE);
    req.setUser(user.getShortName());
    req.setUserGroups(getUserGroups(user));
    return plugin_.evalRowFilterPolicies(req, auditHandler);
  }

  /**
   * This method throws an {@link AuthorizationException} if row filter is enabled on the
   * given tables. This is used to prevent data leak when Hive has row filter enabled but
   * it's disabled in Impala.
   */
  private void throwIfRowFilteringRequired(User user, String dbName, String tableName)
      throws InternalException, AuthorizationException {
    if (evalRowFilter(user, dbName, tableName, null).isRowFilterEnabled()) {
      throw new AuthorizationException(String.format(
          "Row filtering is disabled by --enable_row_filtering flag. Can't access " +
              "table %s.%s that has row filtering policy.",
          dbName, tableName));
    }
  }

  @Override
  public Set<String> getUserGroups(User user) throws InternalException {
    Preconditions.checkNotNull(user);
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
      RangerAccessResourceImpl resource, Authorizable authorizable, User user,
      Privilege privilege) throws InternalException {
    boolean authorized = false;
    RangerBufferAuditHandler originalAuditHandler = authzCtx.getAuditHandler();
    // Use a temporary audit handler instead of the original audit handler
    // so that we know all the audit events generated by the temporary audit
    // handler are contained to the given resource.
    // Recall that in some case, e.g., the DESCRIBE statement, 'originalAuditHandler'
    // could be null, resulting in 'tmpAuditHandler' being null as well.
    RangerBufferAuditHandler tmpAuditHandler =
        (originalAuditHandler == null || !authzCtx.getRetainAudits()) ?
        null : new RangerBufferAuditHandler(originalAuditHandler);
    for (Privilege impliedPrivilege: privilege.getImpliedPrivileges()) {
      if (authorizeResource(authzCtx, user, resource, authorizable, impliedPrivilege,
          tmpAuditHandler)) {
        authorized = true;
        break;
      }
    }
    // 'tmpAuditHandler' could be null if 'originalAuditHandler' is null or
    // authzCtx.getRetainAudits() is false.
    if (originalAuditHandler != null && tmpAuditHandler != null) {
      updateAuditEvents(tmpAuditHandler, originalAuditHandler, true /*any*/,
          privilege);
    }
    return authorized;
  }

  private boolean authorizeAll(RangerAuthorizationContext authzCtx,
      RangerAccessResourceImpl resource, Authorizable authorizable, User user,
      Privilege privilege) throws InternalException {
    boolean authorized = true;
    RangerBufferAuditHandler originalAuditHandler = authzCtx.getAuditHandler();
    // Use a temporary audit handler instead of the original audit handler
    // so that we know all the audit events generated by the temporary audit
    // handler are contained to the given resource.
    // Recall that if 'min_privilege_set_for_show_stmts' is specified when the impalad's
    // are started, this method will be called with the RangerBufferAuditHandler in
    // 'authzCtx' being null for the SHOW DATABASES and SHOW TABLES statements. Refer to
    // BaseAuthorizationChecker#hasAccess(User user, PrivilegeRequest request) for
    // further details.
    RangerBufferAuditHandler tmpAuditHandler =
        (originalAuditHandler == null || !authzCtx.getRetainAudits()) ?
        null : new RangerBufferAuditHandler(originalAuditHandler);
    for (Privilege impliedPrivilege: privilege.getImpliedPrivileges()) {
      if (!authorizeResource(authzCtx, user, resource, authorizable, impliedPrivilege,
          tmpAuditHandler)) {
        authorized = false;
        break;
      }
    }
    // 'tmpAuditHandler' could be null if 'originalAuditHandler' is null or
    // authzCtx.getRetainAudits() is false.
    // Moreover, when 'resource' is associated with a storage handler URI, we pass
    // Privilege.RWSTORAGE to updateAuditEvents() since RWSTORAGE is the only valid
    // access type on a storage handler URI. Otherwise, we may update
    // 'originalAuditHandler' incorrectly since the audit log entries produced by Ranger
    // corresponding to a command like REFRESH/INVALIDATE METADATA has a entry with
    // access type being RWSTORAGE instead of REFRESH.
    if (originalAuditHandler != null && tmpAuditHandler != null) {
      updateAuditEvents(tmpAuditHandler, originalAuditHandler, false /*not any*/,
          resource.getKeys().contains(RangerImpalaResourceBuilder.STORAGE_TYPE) ?
          Privilege.RWSTORAGE : privilege);
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

  private boolean authorizeResource(RangerAuthorizationContext authzCtx, User user,
      RangerAccessResourceImpl resource, Authorizable authorizable, Privilege privilege,
      RangerBufferAuditHandler auditHandler) throws InternalException {
    String accessType;
    // If 'resource' is associated with a storage handler URI, then 'accessType' can only
    // be RWSTORAGE since RWSTORAGE is the only valid privilege that could be applied on
    // a storage handler URI.
    if (resource.getKeys().contains(RangerImpalaResourceBuilder.STORAGE_TYPE)) {
      accessType = Privilege.RWSTORAGE.name().toLowerCase();
    } else {
      if (privilege == Privilege.ANY) {
        accessType = RangerPolicyEngine.ANY_ACCESS;
      } else if (privilege == Privilege.INSERT) {
        // Ranger plugin for Hive considers INSERT to be UPDATE.
        accessType = UPDATE_ACCESS_TYPE;
      } else {
        accessType = privilege.name().toLowerCase();
      }
    }
    RangerAccessRequestImpl request = new RangerAccessRequestImpl();
    request.setResource(resource);
    request.setAccessType(accessType);
    request.setUser(user.getShortName());
    request.setUserGroups(getUserGroups(user));
    request.setClusterName(plugin_.getClusterName());
    if (authzCtx.getSessionState() != null) {
      request.setClientIPAddress(
          authzCtx.getSessionState().getNetwork_address().getHostname());
    }
    RangerAccessResult authorized = plugin_.isAccessAllowed(request, auditHandler);
    if (authorized == null || !authorized.getIsAllowed()) return false;
    // We are done if don't need to block updates on tables that have column-masking or
    // row-filtering policies.
    if (!plugin_.blockUpdateIfTableMaskSpecified() || !privilege.impliesUpdate()
        || (authorizable.getType() != Type.TABLE
           && authorizable.getType() != Type.COLUMN)) {
      return true;
    }
    return authorizeByTableMasking(request, user, authorizable, authorized, privilege,
        auditHandler);
  }

  /**
   * Blocks the update request if any row-filtering or column masking policy exists on the
   * resource(table/column). Appends a deny audit if any exists.
   * Returns true if the request is authorized.
   */
  private boolean authorizeByTableMasking(RangerAccessRequestImpl request, User user,
      Authorizable authorizable, RangerAccessResult accessResult, Privilege privilege,
      RangerBufferAuditHandler auditHandler) throws InternalException {
    Preconditions.checkNotNull(accessResult, "accessResult is null!");
    Preconditions.checkState(accessResult.getIsAllowed(),
        "update should be allowed before checking this");
    String originalAccessType = request.getAccessType();
    // Row-filtering/Column-masking policies are defined only for SELECT requests.
    request.setAccessType(SELECT_ACCESS_TYPE);
    // Check if row filtering is enabled for the table/view.
    if (authorizable.getType() == Type.TABLE) {
      RangerAccessResult rowFilterResult = plugin_.evalRowFilterPolicies(
          request, /*resultProcessor*/null);
      if (rowFilterResult != null && rowFilterResult.isRowFilterEnabled()) {
        LOG.trace("Deny {} on {} due to row filtering policy {}",
            privilege, authorizable.getName(), rowFilterResult.getPolicyId());
        accessResult.setIsAllowed(false);
        accessResult.setPolicyId(rowFilterResult.getPolicyId());
        accessResult.setReason("User does not have access to all rows of the table");
      } else {
        LOG.trace("No row filtering policy found on {}.", authorizable.getName());
      }
    }
    // Check if masking is enabled for any column in the table/view.
    if (accessResult.getIsAllowed()) {
      List<String> columns;
      if (authorizable.getType() == Type.TABLE) {
        // Check all columns.
        columns = ((AuthorizableTable) authorizable).getColumns();
        LOG.trace("Checking mask policies on {} columns of table {}", columns.size(),
            authorizable.getFullTableName());
      } else {
        columns = Lists.newArrayList(authorizable.getColumnName());
      }
      for (String column : columns) {
        RangerAccessResult columnMaskResult = evalColumnMask(user,
            authorizable.getDbName(), authorizable.getTableName(), column,
            /*auditHandler*/null);
        if (columnMaskResult != null && columnMaskResult.isMaskEnabled()) {
          LOG.trace("Deny {} on {} due to column masking policy {}",
              privilege, authorizable.getName(), columnMaskResult.getPolicyId());
          accessResult.setIsAllowed(false);
          accessResult.setPolicyId(columnMaskResult.getPolicyId());
          accessResult.setReason("User does not have access to unmasked column values");
          break;
        } else {
          LOG.trace("No column masking policy found on column {} of {}.", column,
              authorizable.getFullTableName());
        }
      }
    }
    // Set back the original access type. The request object is still referenced by the
    // access result.
    request.setAccessType(originalAccessType);
    // Only add deny audits.
    if (!accessResult.getIsAllowed() && auditHandler != null) {
      auditHandler.processResult(accessResult);
    }
    return accessResult.getIsAllowed();
  }

  @VisibleForTesting
  public RangerImpalaPlugin getRangerImpalaPlugin() { return plugin_; }

  @Override
  public boolean roleExists(String roleName) {
    return RangerUtil.roleExists(plugin_, roleName);
  }
}

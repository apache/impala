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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.authorization.Authorizable.Type;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

/**
 * A base class for the {@link AuthorizationChecker}.
 */
public abstract class BaseAuthorizationChecker implements AuthorizationChecker {
  private final static Logger LOG = LoggerFactory.getLogger(
      BaseAuthorizationChecker.class);

  protected final AuthorizationConfig config_;

  /**
   * Creates a new AuthorizationChecker based on the config values.
   */
  protected BaseAuthorizationChecker(AuthorizationConfig config) {
    Preconditions.checkNotNull(config);
    config_ = config;
  }

  /**
   * Returns true if the given user has permission to execute the given
   * request, false otherwise. Always returns true if authorization is disabled or the
   * given user is an admin user.
   */
  @Override
  public boolean hasAccess(User user, PrivilegeRequest request) throws InternalException {
    // We don't want to do an audit log or profile events logged here. This method is used
    // by "show databases", "show tables", "describe" to filter out unauthorized database,
    // table, or column names.
    return hasAccess(createAuthorizationContext(false /*no audit log*/,
        null /*no SQL statement*/, null /*no session state*/,
        Optional.empty()), user, request);
  }

  private boolean hasAccess(AuthorizationContext authzCtx, User user,
      PrivilegeRequest request) throws InternalException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);

    // If authorization is not enabled the user will always have access. If this is
    // an internal request, the user will always have permission.
    if (!config_.isEnabled() || user instanceof ImpalaInternalAdminUser) {
      return true;
    }
    return authorizeResource(authzCtx, user, request);
  }

  @Override
  public boolean hasAnyAccess(User user, Set<PrivilegeRequest> requests)
      throws InternalException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(requests);

    for (PrivilegeRequest request : requests) {
      if (hasAccess(user, request)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Executes code after the authorization check.
   * Override this method to add custom post-authorization check.
   */
  @Override
  public void postAuthorize(AuthorizationContext authzCtx, boolean authzOk,
      boolean analysisOk) {
    if (authzCtx.getTimeline().isPresent()) {
      EventSequence timeline = authzCtx.getTimeline().get();
      long durationMs = timeline.markEvent(String.format("Authorization finished (%s)",
          config_.getProviderName())) / 1000000;
      LOG.debug("Authorization check took {} ms", durationMs);
    }
  }

  /**
   * Authorize an analyzed statement.
   * analyze() must have already been called. Throws an AuthorizationException if the
   * user doesn't have sufficient privileges to run this statement.
   */
  @Override
  public void authorize(AuthorizationContext authzCtx, AnalysisResult analysisResult,
      FeCatalog catalog) throws AuthorizationException, InternalException {
    Preconditions.checkNotNull(analysisResult);
    Analyzer analyzer = analysisResult.getAnalyzer();
    // Authorize statements that may produce several hierarchical privilege requests.
    // Such a statement always has a corresponding table-level privilege request if it
    // has column-level privilege request. The hierarchical nature requires special
    // logic to process correctly and efficiently.
    if (analysisResult.isHierarchicalAuthStmt()) {
      // Map of table name to a list of privilege requests associated with that table.
      // These include both table-level and column-level privilege requests. We use a
      // LinkedHashMap to preserve the order in which requests are inserted.
      Map<String, List<PrivilegeRequest>> tablePrivReqs = new LinkedHashMap<>();
      // Privilege requests that are not column or table-level.
      List<PrivilegeRequest> otherPrivReqs = new ArrayList<>();
      // Group the registered privilege requests based on the table they reference.
      for (PrivilegeRequest privReq : analyzer.getPrivilegeReqs()) {
        String tableName = privReq.getAuthorizable().getFullTableName();
        if (tableName == null) {
          otherPrivReqs.add(privReq);
        } else {
          List<PrivilegeRequest> requests = tablePrivReqs.get(tableName);
          if (requests == null) {
            requests = new ArrayList<>();
            tablePrivReqs.put(tableName, requests);
          }
          // The table-level SELECT must be the first table-level request, and it
          // must precede all column-level privilege requests.
          Preconditions.checkState((requests.isEmpty() ||
              !(privReq.getAuthorizable().getType() == Authorizable.Type.COLUMN)) ||
              (requests.get(0).getAuthorizable().getType() == Authorizable.Type.TABLE &&
                  requests.get(0).getPrivilege() == Privilege.SELECT));
          requests.add(privReq);
        }
      }

      // Check any non-table, non-column privilege requests first.
      for (PrivilegeRequest request : otherPrivReqs) {
        authorizePrivilegeRequest(authzCtx, analysisResult, catalog, request);
      }

      // Authorize table accesses, one table at a time, by considering both table and
      // column-level privilege requests.
      for (Map.Entry<String, List<PrivilegeRequest>> entry : tablePrivReqs.entrySet()) {
        authorizeTableAccess(authzCtx, analysisResult, catalog, entry.getValue());
      }
    } else {
      for (PrivilegeRequest privReq : analyzer.getPrivilegeReqs()) {
        Preconditions.checkState(
            !(privReq.getAuthorizable().getType() == Authorizable.Type.COLUMN) ||
                analysisResult.isSingleColumnPrivStmt());
        authorizePrivilegeRequest(authzCtx, analysisResult, catalog, privReq);
      }
    }

    // Check all masked requests. If a masked request has an associated error message,
    // an AuthorizationException is thrown if authorization fails. Masked requests with
    // no error message are used to check if the user can access the runtime profile.
    // These checks don't result in an AuthorizationException but set the
    // 'user_has_profile_access' flag in queryCtx_.
    for (Pair<PrivilegeRequest, String> maskedReq : analyzer.getMaskedPrivilegeReqs()) {
      try {
        authzCtx.setRetainAudits(false);
        authorizePrivilegeRequest(authzCtx, analysisResult, catalog, maskedReq.first);
      } catch (AuthorizationException e) {
        analysisResult.setUserHasProfileAccess(false);
        if (!Strings.isNullOrEmpty(maskedReq.second)) {
          throw new AuthorizationException(maskedReq.second);
        }
        break;
      } finally {
        authzCtx.setRetainAudits(true);
      }
    }
  }

  /**
   * Authorize a privilege request.
   * Throws an AuthorizationException if the user doesn't have sufficient privileges for
   * this request. Also, checks if the request references a system database.
   */
  private void authorizePrivilegeRequest(AuthorizationContext authzCtx,
      AnalysisResult analysisResult, FeCatalog catalog, PrivilegeRequest request)
      throws AuthorizationException, InternalException {
    Preconditions.checkNotNull(request);
    String dbName = null;
    if (request.getAuthorizable() != null) {
      dbName = request.getAuthorizable().getDbName();
    }
    // If this is a system database, some actions should always be allowed
    // or disabled, regardless of what is in the auth policy.
    if (dbName != null && checkSystemDbAccess(catalog, dbName, request.getPrivilege())) {
      return;
    }
    // Populate column names to check column masking policies in blocking updates.
    // No need to do this for REFRESH if allow_catalog_cache_op_from_masked_users=true.
    // Note that db.getTable() could be a heavy operation in local catalog mode since it
    // triggers metadata loading on the table if it's unloaded in catalogd. Skipping this
    // improves the performance of "INVALIDATE METADATA <table>" statements. For REFRESH
    // statements, the performance doesn't differ a lot since there are other places that
    // use db.getTable() (see IMPALA-12591).
    if (config_.isEnabled() && request.getAuthorizable() != null
        && request.getAuthorizable().getType() == Type.TABLE
        && (request.getPrivilege() != Privilege.REFRESH
          || !BackendConfig.INSTANCE.allowCatalogCacheOpFromMaskedUsers())) {
      Preconditions.checkNotNull(dbName);
      AuthorizableTable authorizableTable = (AuthorizableTable) request.getAuthorizable();
      FeDb db = catalog.getDb(dbName);
      if (db != null) {
        // 'db', 'table' could be null for an unresolved table ref. 'table' could be
        // null for target table of a CTAS statement. Don't need to populate column
        // names in such cases since no column masking policies will be checked.
        FeTable table = db.getTable(authorizableTable.getTableName());
        if (table != null && !(table instanceof FeIncompleteTable)) {
          authorizableTable.setColumns(table.getColumnNames());
        }
      }
    }
    checkAccess(authzCtx, analysisResult.getAnalyzer().getUser(), request);
  }

  /**
   * Authorize a list of privilege requests associated with a single table.
   * It checks if the user has sufficient table-level privileges and if that is
   * not the case, it falls back on checking column-level privileges, if any. This
   * function requires 'SELECT' requests to be ordered by table and then by column
   * privilege requests. Throws an AuthorizationException if the user doesn't have
   * sufficient privileges.
   */
  protected void authorizeTableAccess(AuthorizationContext authzCtx,
      AnalysisResult analysisResult, FeCatalog catalog, List<PrivilegeRequest> requests)
      throws AuthorizationException, InternalException {
    Preconditions.checkArgument(!requests.isEmpty());
    Analyzer analyzer = analysisResult.getAnalyzer();
    // Deny access of columns containing column masking policies when column masking
    // feature is disabled. Deny access of the tables containing row filtering policies
    // when row filtering feature is disabled. This is to prevent data leak since we do
    // not want Impala to show any information when these features are disabled.
    authorizeRowFilterAndColumnMask(analyzer.getUser(), requests);

    boolean hasTableSelectPriv = true;
    boolean hasColumnSelectPriv = false;
    for (PrivilegeRequest request: requests) {
      if (request.getAuthorizable().getType() == Authorizable.Type.TABLE) {
        try {
          authorizePrivilegeRequest(authzCtx, analysisResult, catalog, request);
        } catch (AuthorizationException e) {
          // Authorization fails if we fail to authorize any table-level request that is
          // not a SELECT privilege (e.g. INSERT).
          if (request.getPrivilege() != Privilege.SELECT) throw e;
          hasTableSelectPriv = false;
        }
      } else {
        Preconditions.checkState(
            request.getAuthorizable().getType() == Authorizable.Type.COLUMN);
        // In order to support deny policies on columns
        if (hasTableSelectPriv &&
                request.getPrivilege() != Privilege.SELECT &&
                request.getPrivilege() != Privilege.INSERT) {
          continue;
        }
        if (hasAccess(authzCtx, analyzer.getUser(), request)) {
          hasColumnSelectPriv = true;
          continue;
        }
        // Make sure we don't reveal any column names in the error message.
        throw new AuthorizationException(String.format("User '%s' does not have " +
                "privileges to execute '%s' on: %s", analyzer.getUser().getName(),
            request.getPrivilege().toString(),
            request.getAuthorizable().getFullTableName()));
      }
    }
    if (!hasTableSelectPriv && !hasColumnSelectPriv) {
      throw new AuthorizationException(String.format("User '%s' does not have " +
              "privileges to execute 'SELECT' on: %s", analyzer.getUser().getName(),
          requests.get(0).getAuthorizable().getFullTableName()));
    }
  }

  /**
   * Throws an AuthorizationException if the dbName is a system db
   * and the user is trying to modify it.
   * Returns true if this is a system db and the action is allowed.
   * Return false if authorization should be checked in the usual way.
   */
  private boolean checkSystemDbAccess(FeCatalog catalog, String dbName,
      Privilege privilege)
      throws AuthorizationException {
    FeDb db = catalog.getDb(dbName);
    if (db != null && db.isSystemDb()) {
      switch (privilege) {
        case VIEW_METADATA:
        case ANY:
          return true;
        case SELECT:
          // Check authorization for SELECT on system tables in the usual way.
          return false;
        default:
          throw new AuthorizationException("Cannot modify system database.");
      }
    }
    return false;
  }

  /**
   * Authorizes the PrivilegeRequest, throwing an Authorization exception if
   * the user does not have sufficient privileges.
   */
  private void checkAccess(AuthorizationContext authzCtx, User user,
      PrivilegeRequest privilegeRequest)
      throws AuthorizationException, InternalException {
    Preconditions.checkNotNull(privilegeRequest);

    if (hasAccess(authzCtx, user, privilegeRequest)) return;

    Privilege privilege = privilegeRequest.getPrivilege();
    if (privilegeRequest.getAuthorizable().getType() == Type.FUNCTION) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to %s functions in: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getName()));
    }

    if (EnumSet.of(Privilege.ANY, Privilege.ALL, Privilege.VIEW_METADATA)
        .contains(privilege)) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to access: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()),
          privilegeRequest.getName()));
    } else if (privilege == Privilege.REFRESH) {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute " +
              "'INVALIDATE METADATA/REFRESH' on: %s", user.getName(),
          grantOption(privilegeRequest.hasGrantOption()), privilegeRequest.getName()));
    } else if (privilege == Privilege.CREATE &&
        privilegeRequest.getAuthorizable().getType() == Type.TABLE) {
      // Creating a table requires CREATE on a database and we shouldn't
      // expose the table name.
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute '%s' on: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getAuthorizable().getDbName()));
    } else {
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges%s to execute '%s' on: %s",
          user.getName(), grantOption(privilegeRequest.hasGrantOption()), privilege,
          privilegeRequest.getName()));
    }
  }

  private static String grantOption(boolean hasGrantOption) {
    return hasGrantOption ? " with 'GRANT OPTION'" : "";
  }

  /**
   * Performs an authorization for a given user and resource.
   */
  protected abstract boolean authorizeResource(AuthorizationContext authzCtx, User user,
      PrivilegeRequest request) throws InternalException;

  /**
   * Returns a set of groups for a given user.
   */
  public abstract Set<String> getUserGroups(User user) throws InternalException;

  /**
   * Checks if the given tables/columns are configured with row filtering/column masking
   * enabled. If they do, throw an {@link AuthorizationException} to prevent data leak.
   *
   * TODO: Remove this method when Impala supports row filtering and column masking.
   */
  protected abstract void authorizeRowFilterAndColumnMask(User user,
      List<PrivilegeRequest> privilegeRequests)
      throws AuthorizationException, InternalException;

  /**
   * Invalidates an authorization cache.
   */
  public abstract void invalidateAuthorizationCache();
}

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.util.ClassUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs.AccessResult;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of {@link AuthorizationManager} for Impalad using Ranger.
 *
 * Operations here make requests to Ranger via the {@link RangerImpalaPlugin} to
 * manage privileges for users.
 *
 * Operations not supported by Ranger will throw an {@link UnsupportedFeatureException}.
 */
public class RangerImpaladAuthorizationManager implements AuthorizationManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      RangerImpaladAuthorizationManager.class);
  private static final String ANY = "*";

  private final Supplier<RangerImpalaPlugin> plugin_;

  public RangerImpaladAuthorizationManager(Supplier<RangerImpalaPlugin> pluginSupplier) {
    plugin_ = pluginSupplier;
  }

  @Override
  public void createRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  /**
   * This method will be called for the statements of 1) SHOW ROLES,
   * 2) SHOW CURRENT ROLES, or 3) SHOW ROLE GRANT GROUP <group_name>.
   */
  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    try {
      TShowRolesResult result = new TShowRolesResult();
      Set<String> groups = RangerUtil.getGroups(params.getRequesting_user());

      boolean adminOp =
          !(groups.contains(params.getGrant_group()) || params.is_show_current_roles);

      if (adminOp) {
        RangerUtil.validateRangerAdmin(plugin_.get(), params.getRequesting_user());
      }

      // The branch for SHOW CURRENT ROLES and SHOW ROLE GRANT GROUP.
      Set<String> roleNames;
      if (params.isIs_show_current_roles() || params.isSetGrant_group()) {
        Set<String> groupNames;
        if (params.isIs_show_current_roles()) {
          groupNames = groups;
        } else {
          Preconditions.checkState(params.isSetGrant_group());
          groupNames = Sets.newHashSet(params.getGrant_group());
        }
        roleNames = plugin_.get().getRolesFromUserAndGroups(null, groupNames);
      } else {
        // The branch for SHOW ROLES.
        Preconditions.checkState(!params.isIs_show_current_roles());
        Set<RangerRole> roles = plugin_.get().getRoles().getRangerRoles();
        if (null == roles) {
          roles = Collections.emptySet();
        }
        roleNames = roles.stream().map(RangerRole::getName).collect(Collectors.toSet());
      }

      // Need to instantiate the field of 'role_names' in 'result' since its field of
      // 'role_names' was initialized as null by default.
      result.setRole_names(Lists.newArrayList(roleNames));
      Collections.sort(result.getRole_names());
      return result;
    } catch (Exception e) {
      if (params.is_show_current_roles) {
        LOG.error("Error executing SHOW CURRENT ROLES.", e);
        throw new InternalException("Error executing SHOW CURRENT ROLES."
            + " Ranger error message: " + e.getMessage());
      } else if (params.isSetGrant_group()) {
        LOG.error("Error executing SHOW ROLE GRANT GROUP " + params.getGrant_group() +
            ".");
        throw new InternalException("Error executing SHOW ROLE GRANT GROUP "
            + params.getGrant_group() + ". Ranger error message: " + e.getMessage());
      } else {
        LOG.error("Error executing SHOW ROLES.");
        throw new InternalException("Error executing SHOW ROLES."
            + " Ranger error message: " + e.getMessage());
      }
    }
  }

  @Override
  public void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromRole(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromUser(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromGroup(TCatalogServiceRequestHeader header,
      TGrantRevokePrivParams params, TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  private static Optional<String> getResourceName(String resourceType,
      String resourceName, AccessResult accessResult) {
    RangerPolicy.RangerPolicyResource rangerPolicyResource =
        accessResult.getPolicy().getResources().get(resourceType);

    if (rangerPolicyResource == null) {
      return Optional.empty();
    }
    boolean nameIsPresent = rangerPolicyResource.getValues().contains(resourceName);

    return nameIsPresent ? Optional.of(resourceName) : Optional.of(ANY);
  }

  private static boolean isDelegateAdmin(AccessResult accessResult, String privilegeLevel,
      String principal, TPrincipalType type) {
    for (RangerPolicy.RangerPolicyItem item : accessResult.getPolicy().getPolicyItems()) {
      switch (type) {
        case USER:
          if (item.getUsers().contains(principal) &&
              item.getAccesses().stream()
                  .anyMatch(rpia -> rpia.getType().equals(privilegeLevel))) {
            return item.getDelegateAdmin();
          }
          break;
        case GROUP:
          if (item.getGroups().contains(principal)) {
            return item.getDelegateAdmin();
          }
          break;
        case ROLE:
          if (item.getRoles().contains(principal)) {
            return item.getDelegateAdmin();
          }
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unsupported principal " +
              "type %s", type));
      }
    }
    return false;
  }

  private static RangerResultRow toResultRow(String rangerPrivilegeLevel,
      String principal, TPrincipalType type, AccessResult accessResult,
      TPrivilege privilege) {
    TPrivilege rangerPrivilege = new TPrivilege();
    rangerPrivilege.setScope(privilege.getScope());
    boolean grantOption = isDelegateAdmin(accessResult, rangerPrivilegeLevel, principal,
        type);

    // Ignore hive privileges which may not exist.
    TPrivilegeLevel level;
    try {
      level = TPrivilegeLevel.valueOf(rangerPrivilegeLevel.toUpperCase());
    } catch (IllegalArgumentException e) {
      if (rangerPrivilegeLevel.equals(RangerAuthorizationChecker.UPDATE_ACCESS_TYPE)) {
        level = TPrivilegeLevel.INSERT;
      } else {
        return null;
      }
    }

    Date createTime = accessResult.getPolicy().getCreateTime();
    Long longTime = createTime == null ? null : createTime.getTime();

    Optional<String> database = getResourceName(RangerImpalaResourceBuilder.DATABASE,
        privilege.getDb_name(), accessResult);
    Optional<String> table = getResourceName(RangerImpalaResourceBuilder.TABLE,
        privilege.getTable_name(), accessResult);
    Optional<String> column = getResourceName(RangerImpalaResourceBuilder.COLUMN,
        privilege.getColumn_name(), accessResult);
    Optional<String> uri = getResourceName(RangerImpalaResourceBuilder.URL,
        privilege.getUri(), accessResult);
    Optional<String> storageType = getResourceName(
        RangerImpalaResourceBuilder.STORAGE_TYPE, privilege.getStorage_type(),
        accessResult);
    Optional<String> storageUri = getResourceName(
        RangerImpalaResourceBuilder.STORAGE_URL, privilege.getStorage_url(),
        accessResult);
    Optional<String> udf = getResourceName(RangerImpalaResourceBuilder.UDF,
        privilege.getFn_name(), accessResult);

    return new RangerResultRow(type, principal, database.orElse(""), table.orElse(""),
        column.orElse(""), uri.orElse(""), storageType.orElse(""), storageUri.orElse(""),
        udf.orElse(""), level, grantOption, longTime);
  }

  private static List<RangerAccessRequest> buildAccessRequests(TPrivilege privilege) {
    List<Map<String, String>> resources = new ArrayList<>();

    if (privilege == null) {
      throw new UnsupportedOperationException("SHOW GRANT is not supported without a" +
          " defined resource in Ranger.");
    }

    if (privilege.getColumn_name() != null || privilege.getTable_name() != null) {
      resources.add(RangerUtil.createColumnResource(privilege));
    } else if (privilege.getUri() != null) {
      resources.add(RangerUtil.createUriResource(privilege));
    } else if (privilege.getFn_name() != null) {
      resources.add(RangerUtil.createFunctionResource(privilege));
    } else if (privilege.getDb_name() != null) {
      // DB is used by column and function resources.
      resources.add(RangerUtil.createColumnResource(privilege));
      resources.add(RangerUtil.createFunctionResource(privilege));
    } else if (privilege.getStorage_url() != null ||
        privilege.getStorage_type() != null) {
      resources.add(RangerUtil.createStorageHandlerUriResource(privilege));
    } else {
      // Server is used by column, function, URI, and storage handler URI resources.
      resources.add(RangerUtil.createColumnResource(privilege));
      resources.add(RangerUtil.createUriResource(privilege));
      resources.add(RangerUtil.createStorageHandlerUriResource(privilege));
      resources.add(RangerUtil.createFunctionResource(privilege));
    }

    List<RangerAccessRequest> requests = new ArrayList<>();
    for (Map<String, String> resource : resources) {
      RangerAccessRequestImpl request = new RangerAccessRequestImpl();
      request.setResource(
          new RangerAccessResourceImpl(Collections.unmodifiableMap(resource)));
      request.setAccessType(RangerPolicyEngine.ANY_ACCESS);
      requests.add(request);
    }
    return requests;
  }

  private static List<RangerResultRow> aclToPrivilege(Map<String, AccessResult> acls,
      String principal, TPrivilege privilege, TPrincipalType type) {
    return acls.entrySet()
        .stream()
        .map(en -> toResultRow(en.getKey(), principal, type, en.getValue(), privilege))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public TResultSet getPrivileges(TShowGrantPrincipalParams params)
      throws ImpalaException {
    List<RangerAccessRequest> requests = buildAccessRequests(params.privilege);
    Set<TResultRow> resultSet = new TreeSet<>();
    TResultSet result = new TResultSet();

    result.setSchema(RangerResultRow.getSchema());
    result.setRows(new ArrayList<>());

    for (RangerAccessRequest request : requests) {
      List<RangerResultRow> resultRows;
      RangerResourceACLs acls = plugin_.get().getResourceACLs(request);

      switch (params.principal_type) {
        case USER:
          resultRows = new ArrayList<>(aclToPrivilege(
              acls.getUserACLs().getOrDefault(params.name, Collections.emptyMap()),
              params.name, params.privilege, TPrincipalType.USER));
          for (String group : RangerUtil.getGroups(params.name)) {
            resultRows.addAll(aclToPrivilege(
                acls.getGroupACLs().getOrDefault(group, Collections.emptyMap()),
                params.name, params.privilege, TPrincipalType.GROUP));
          }
          break;
        case GROUP:
          resultRows = new ArrayList<>(aclToPrivilege(
              acls.getGroupACLs().getOrDefault(params.name, Collections.emptyMap()),
              params.name, params.privilege, TPrincipalType.GROUP));
          break;
        case ROLE:
          resultRows = new ArrayList<>(aclToPrivilege(
              acls.getRoleACLs().getOrDefault(params.name, Collections.emptyMap()),
              params.name, params.privilege, TPrincipalType.ROLE));
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unsupported principal " +
              "type %s.", params.principal_type));
      }

      RangerResourceResult resourceResult = new RangerResourceResult();

      // Categorize 'resultRows' based on their lowest non-wildcard resource in the
      // resource hierarchy. RangerResultRow's falling into the same category correspond
      // to the same resource.
      // TODO: To support displaying privileges on UDF's later.
      for (RangerResultRow row : resultRows) {
        if (!row.column_.equals("*") && !row.column_.isEmpty()) {
          resourceResult.addColumnResult(row);
        } else if (!row.table_.equals("*") && !row.table_.isEmpty()) {
          resourceResult.addTableResult(row);
        } else if (!row.udf_.equals("*") && !row.udf_.isEmpty()) {
          resourceResult.addUdfResult(row);
        } else if (!row.database_.equals("*") && !row.database_.isEmpty()) {
          resourceResult.addDatabaseResult(row);
        } else {
          resourceResult.addServerResult(row);
        }
      }

      resourceResult.getResultRows()
          .forEach(principal -> resultSet.add(principal.toResultRow()));
    }
    resultSet.forEach(result::addToRows);

    return result;
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
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  private static class RangerResourceResult {
    private List<RangerResultRow> server = new ArrayList<>();
    private List<RangerResultRow> database = new ArrayList<>();
    private List<RangerResultRow> table = new ArrayList<>();
    private List<RangerResultRow> column = new ArrayList<>();
    private List<RangerResultRow> udf = new ArrayList<>();

    public RangerResourceResult() { }

    public RangerResourceResult addServerResult(RangerResultRow result) {
      server.add(result);
      return this;
    }

    public RangerResourceResult addDatabaseResult(RangerResultRow result) {
      database.add(result);
      return this;
    }

    public RangerResourceResult addTableResult(RangerResultRow result) {
      table.add(result);
      return this;
    }

    public RangerResourceResult addColumnResult(RangerResultRow result) {
      column.add(result);
      return this;
    }

    public RangerResourceResult addUdfResult(RangerResultRow result) {
      udf.add(result);
      return this;
    }

    /**
     * For each disjoint List corresponding to a given resource, if there exists a
     * RangerResultRow indicating the specified principal's privilege of
     * TPrivilegeLevel.ALL, we filter out other RangerResultRow's that could be deduced
     * from this wildcard RangerResultRow.
     */
    public List<RangerResultRow> getResultRows() {
      List<RangerResultRow> results = new ArrayList<>();

      results.addAll(filterIfAll(server));
      results.addAll(filterIfAll(database));
      results.addAll(filterIfAll(table));
      results.addAll(filterIfAll(column));
      results.addAll(filterIfAll(udf));
      return results;
    }

    /**
     * Given that the elements on 'resultRow' refer to the same resource, in the case
     * when any of the granted privileges on this resource equals 'TPrivilegeLevel.ALL',
     * we only keep this wildcard RangerResultRow since any other RangerResultRow in
     * 'resultRow' could be inferred from this wildcard RangerResultRow.
     */
    private static List<RangerResultRow> filterIfAll(List<RangerResultRow> resultRows) {
      boolean all = resultRows.stream().anyMatch(row ->
          row.privilege_ == TPrivilegeLevel.ALL);

      List<RangerResultRow> rows = all ? resultRows.stream()
          .filter(row -> row.privilege_ == TPrivilegeLevel.ALL)
          .collect(Collectors.toList()) : resultRows;

      return rows;
    }
  }

  private static class RangerResultRow {
    private final TPrincipalType principalType_;
    private final String principalName_;
    private final String database_;
    private final String table_;
    private final String column_;
    private final String uri_;
    private final String storageType_;
    private final String storageUri_;
    private final String udf_;
    private final TPrivilegeLevel privilege_;
    private final boolean grantOption_;
    private final Long createTime_;

    public RangerResultRow(TPrincipalType principalType, String principalName,
        String database, String table, String column, String uri, String storageType,
        String storageUri, String udf, TPrivilegeLevel privilege, boolean grantOption,
        Long createTime) {
      this.principalType_ = principalType;
      this.principalName_ = principalName;
      this.database_ = database;
      this.table_ = table;
      this.column_ = column;
      this.uri_ = uri;
      this.storageType_ = storageType;
      this.storageUri_ = storageUri;
      this.udf_ = udf;
      this.privilege_ = privilege;
      this.grantOption_ = grantOption;
      this.createTime_ = createTime;
    }

    public static TResultSetMetadata getSchema() {
      TResultSetMetadata schema = new TResultSetMetadata();

      schema.addToColumns(new TColumn("principal_type", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("principal_name", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("database", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("table", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("column", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("uri", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("storage_type", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("storage_uri", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("udf", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("privilege", Type.STRING.toThrift()));
      schema.addToColumns(new TColumn("grant_option", Type.BOOLEAN.toThrift()));
      schema.addToColumns(new TColumn("create_time", Type.STRING.toThrift()));

      return schema;
    }

    public TResultRow toResultRow() {
      TResultRowBuilder rowBuilder = new TResultRowBuilder();

      rowBuilder.add(principalType_.name().toUpperCase());
      rowBuilder.add(principalName_);
      rowBuilder.add(database_);
      rowBuilder.add(table_);
      rowBuilder.add(column_);
      rowBuilder.add(uri_);
      rowBuilder.add(storageType_);
      rowBuilder.add(storageUri_);
      rowBuilder.add(udf_);
      rowBuilder.add(privilege_.name().toLowerCase());
      rowBuilder.add(grantOption_);
      if (createTime_ == null) {
        rowBuilder.add(null);
      } else {
        rowBuilder.add(createTime_);
      }

      return rowBuilder.get();
    }
  }
}

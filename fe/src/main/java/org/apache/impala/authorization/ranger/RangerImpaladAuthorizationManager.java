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

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.impala.authorization.AuthorizationDelta;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;
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
import org.apache.ranger.plugin.service.RangerAuthContext;

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
  private static final String ANY = "*";

  private final Supplier<RangerImpalaPlugin> plugin_;
  private final Supplier<RangerAuthContext> authContext_;

  public RangerImpaladAuthorizationManager(Supplier<RangerImpalaPlugin> pluginSupplier) {
    plugin_ = pluginSupplier;
    authContext_ = () -> plugin_.get().createRangerAuthContext();
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

  @Override
  public TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException {
    if (params.getGrant_group() != null) {
      throw new UnsupportedFeatureException(
          "SHOW ROLE GRANT GROUP is not supported by Ranger.");
    }
    throw new UnsupportedFeatureException(
        String.format("SHOW %sROLES is not supported by Ranger.",
            params.is_show_current_roles ? "CURRENT " : ""));
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
  public void grantPrivilegeToRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToUser(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromUser(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void grantPrivilegeToGroup(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  @Override
  public void revokePrivilegeFromGroup(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException {
    throw new UnsupportedOperationException(String.format(
        "%s is not supported in Impalad", ClassUtil.getMethodName()));
  }

  private static Set<String> getGroups(String principal) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(principal);
    return Sets.newHashSet(ugi.getGroupNames());
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
    Optional<String> udf = getResourceName(RangerImpalaResourceBuilder.UDF, ANY,
        accessResult);

    switch (privilege.getScope()) {
      case COLUMN:
        if (!column.isPresent() || column.get().equals("*")) return null;
      case TABLE:
        if (!table.isPresent() || table.get().equals("*")) return null;
      case DATABASE:
        if (!database.isPresent() || database.get().equals("*")) return null;
        break;
      case URI:
        if (!uri.isPresent() || uri.get().equals("*")) return null;
        break;
      case SERVER:
        break;
      default:
        throw new IllegalArgumentException("Unsupported privilege scope " +
            privilege.getScope());
    }

    return new RangerResultRow(type, principal, database.orElse(""), table.orElse(""),
        column.orElse(""), uri.orElse(""), udf.orElse(""), level, grantOption, longTime);
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
    } else if (privilege.getDb_name() != null) {
      // DB is used by column and function resources.
      resources.add(RangerUtil.createColumnResource(privilege));
      resources.add(RangerUtil.createFunctionResource(privilege));
    } else {
      // Server is used by column, function, and URI resources.
      resources.add(RangerUtil.createColumnResource(privilege));
      resources.add(RangerUtil.createUriResource(privilege));
      resources.add(RangerUtil.createFunctionResource(privilege));
    }

    List<RangerAccessRequest> requests = new ArrayList<>();
    for (Map<String, String> resource : resources) {
      requests.add(new RangerAccessRequestImpl(
          new RangerAccessResourceImpl(Collections.unmodifiableMap(resource)),
          RangerPolicyEngine.ANY_ACCESS, null, null));
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
    if (params.principal_type == TPrincipalType.ROLE) {
      throw new UnsupportedFeatureException(
          "SHOW GRANT ROLE is not supported by Ranger.");
    }

    List<RangerAccessRequest> requests = buildAccessRequests(params.privilege);
    Set<TResultRow> resultSet = new TreeSet<>();
    TResultSet result = new TResultSet();

    result.setSchema(RangerResultRow.getSchema());
    result.setRows(new ArrayList<>());

    for (RangerAccessRequest request : requests) {
      List<RangerResultRow> resultRows;
      RangerResourceACLs acls = authContext_.get().getResourceACLs(request);

      switch (params.principal_type) {
        case USER:
          resultRows = new ArrayList<>(aclToPrivilege(
              acls.getUserACLs().getOrDefault(params.name, Collections.emptyMap()),
              params.name, params.privilege, TPrincipalType.USER));
          for (String group : getGroups(params.name)) {
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
        default:
          throw new UnsupportedOperationException(String.format("Unsupported principal " +
              "type %s.", params.principal_type));
      }

      boolean all = resultRows.stream().anyMatch(row ->
          row.privilege_ == TPrivilegeLevel.ALL);

      List<RangerResultRow> rows = all ? resultRows.stream()
          .filter(row -> row.privilege_ == TPrivilegeLevel.ALL)
          .collect(Collectors.toList()) : resultRows;

      rows.forEach(principal -> resultSet.add(principal.toResultRow()));
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
    // TODO: IMPALA-8293 (part 2)
    return new AuthorizationDelta();
  }

  private static class RangerResultRow {
    private final TPrincipalType principalType_;
    private final String principalName_;
    private final String database_;
    private final String table_;
    private final String column_;
    private final String uri_;
    private final String udf_;
    private final TPrivilegeLevel privilege_;
    private final boolean grantOption_;
    private final Long createTime_;

    public RangerResultRow(TPrincipalType principalType, String principalName,
        String database, String table, String column, String uri, String udf,
        TPrivilegeLevel privilege, boolean grantOption, Long createTime) {
      this.principalType_ = principalType;
      this.principalName_ = principalName;
      this.database_ = database;
      this.table_ = table;
      this.column_ = column;
      this.uri_ = uri;
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

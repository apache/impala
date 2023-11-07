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

package org.apache.impala.util;

import org.apache.impala.analysis.GrantRevokePrivStmt;
import org.apache.impala.analysis.GrantRevokeRoleStmt;
import org.apache.impala.analysis.PrivilegeSpec;
import org.apache.impala.analysis.ResetMetadataStmt;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.User;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbType;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TColumnName;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TCreateOrAlterViewParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionName;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TTableName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class CatalogOpUtilTest {

  @BeforeClass
  public static void setup() {
    // Make sure BackendConfig.INSTANCE is initialized.
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
  }

  private void testResetStmt(ResetMetadataStmt stmt, User user, String expected)
      throws Exception {
    stmt.setRequestingUser(user);
    assertEquals(expected, CatalogOpUtil.getShortDescForReset(stmt.toThrift()));
  }

  @Test
  public void testResetMetadataDesc() throws Exception {
    User user = new User("Alice");
    TableName tblName = new TableName("default", "tbl");

    testResetStmt(ResetMetadataStmt.createInvalidateStmt(),
        user, "INVALIDATE ALL issued by Alice");
    testResetStmt(ResetMetadataStmt.createInvalidateStmt(tblName),
        user, "INVALIDATE TABLE default.tbl issued by Alice");
    testResetStmt(ResetMetadataStmt.createRefreshTableStmt(tblName),
        user, "REFRESH TABLE default.tbl issued by Alice");
    testResetStmt(ResetMetadataStmt.createRefreshFunctionsStmt("db1"),
        user, "REFRESH FUNCTIONS IN DATABASE db1 issued by Alice");
    testResetStmt(ResetMetadataStmt.createRefreshAuthorizationStmt(),
        user, "REFRESH AUTHORIZATION issued by Alice");

    // Test REFRESH PARTITIONS using a fake partition spec
    ResetMetadataStmt stmt = ResetMetadataStmt.createRefreshTableStmt(tblName);
    stmt.setRequestingUser(user);
    TResetMetadataRequest req = stmt.toThrift();
    req.setPartition_spec(Collections.emptyList());
    assertEquals("REFRESH TABLE default.tbl PARTITIONS issued by Alice",
        CatalogOpUtil.getShortDescForReset(req));
  }

  @Test
  public void testDdlDesc() {
    TDdlExecRequest req;
    TTableName tblName = new TTableName("db1", "tbl1");

    req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.CREATE_DATABASE);
    TCreateDbParams createDbParams = new TCreateDbParams();
    createDbParams.setDb("db1");
    req.setCreate_db_params(createDbParams);
    assertEquals("CREATE_DATABASE db1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.DROP_DATABASE);
    TDropDbParams dropDbParams = new TDropDbParams();
    dropDbParams.setDb("db1");
    req.setDrop_db_params(dropDbParams);
    assertEquals("DROP_DATABASE db1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.ALTER_DATABASE);
    TAlterDbParams alterDbParams = new TAlterDbParams();
    alterDbParams.setAlter_type(TAlterDbType.SET_OWNER);
    alterDbParams.setDb("db1");
    req.setAlter_db_params(alterDbParams);
    assertEquals("ALTER_DATABASE db1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.CREATE_TABLE);
    TCreateTableParams createTableParams = new TCreateTableParams();
    createTableParams.setTable_name(tblName);
    req.setCreate_table_params(createTableParams);
    assertEquals("CREATE_TABLE db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.CREATE_TABLE_AS_SELECT);
    assertEquals("CREATE_TABLE_AS_SELECT db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(tblName);
    req.setAlter_table_params(alterTableParams);
    assertEquals("ALTER_TABLE db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.CREATE_VIEW);
    TCreateOrAlterViewParams alterViewParams = new TCreateOrAlterViewParams();
    alterViewParams.setView_name(tblName);
    req.setCreate_view_params(alterViewParams);
    assertEquals("CREATE_VIEW db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.ALTER_VIEW);
    req.setAlter_view_params(alterViewParams);
    assertEquals("ALTER_VIEW db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.DROP_TABLE);
    TDropTableOrViewParams dropTableOrViewParams = new TDropTableOrViewParams();
    dropTableOrViewParams.setTable_name(tblName);
    req.setDrop_table_or_view_params(dropTableOrViewParams);
    assertEquals("DROP_TABLE db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.DROP_VIEW);
    assertEquals("DROP_VIEW db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.COMMENT_ON);
    TCommentOnParams commentOnParams = new TCommentOnParams();
    commentOnParams.setDb("db1");
    req.setComment_on_params(commentOnParams);
    assertEquals("COMMENT_ON DB db1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    commentOnParams.clear();
    commentOnParams.setTable_name(tblName);
    req.setComment_on_params(commentOnParams);
    assertEquals("COMMENT_ON TABLE db1.tbl1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    commentOnParams.clear();
    commentOnParams.setColumn_name(new TColumnName(tblName, "col1"));
    req.setComment_on_params(commentOnParams);
    assertEquals("COMMENT_ON COLUMN db1.tbl1.col1 issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.CREATE_FUNCTION);
    TCreateFunctionParams createFunctionParams = new TCreateFunctionParams();
    TFunction fn = new TFunction();
    fn.setName(new TFunctionName("my_func"));
    createFunctionParams.setFn(fn);
    req.setCreate_fn_params(createFunctionParams);
    assertEquals("CREATE_FUNCTION my_func issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.DROP_FUNCTION);
    TDropFunctionParams dropFunctionParams = new TDropFunctionParams();
    dropFunctionParams.setFn_name(new TFunctionName("my_func"));
    req.setDrop_fn_params(dropFunctionParams);
    assertEquals("DROP_FUNCTION my_func issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.CREATE_ROLE);
    TCreateDropRoleParams createDropRoleParams = new TCreateDropRoleParams();
    createDropRoleParams.setRole_name("my_role");
    req.setCreate_drop_role_params(createDropRoleParams);
    assertEquals("CREATE_ROLE my_role issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.DROP_ROLE);
    assertEquals("DROP_ROLE my_role issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.GRANT_ROLE);
    TGrantRevokeRoleParams grantRevokeRoleParams;
    grantRevokeRoleParams = new GrantRevokeRoleStmt("my_role", "my_group", true)
        .toThrift();
    req.setGrant_revoke_role_params(grantRevokeRoleParams);
    assertEquals("GRANT_ROLE [my_role] GROUP [my_group] issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.REVOKE_ROLE);
    assertEquals("REVOKE_ROLE [my_role] GROUP [my_group] issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));

    req.setDdl_type(TDdlType.GRANT_PRIVILEGE);
    TGrantRevokePrivParams grantRevokePrivParams = new GrantRevokePrivStmt("my_role",
        PrivilegeSpec.createServerScopedPriv(TPrivilegeLevel.SELECT), true, false,
        TPrincipalType.ROLE).toThrift();
    req.setGrant_revoke_priv_params(grantRevokePrivParams);
    assertEquals("GRANT_PRIVILEGE TO my_role issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
    req.setDdl_type(TDdlType.REVOKE_PRIVILEGE);
    assertEquals("REVOKE_PRIVILEGE FROM my_role issued by unknown user",
        CatalogOpUtil.getShortDescForExecDdl(req));
  }
}

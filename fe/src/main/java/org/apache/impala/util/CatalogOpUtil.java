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

import org.apache.impala.analysis.ColumnName;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCommentOnParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogOpUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogOpUtil.class);

  /**
   * Get a short description for the execDdl request.
   */
  public static String getShortDescForExecDdl(TDdlExecRequest req) {
    String target;
    try {
      switch (req.ddl_type) {
        case ALTER_DATABASE:
          target = req.getAlter_db_params().getDb();
          break;
        case ALTER_TABLE:
          target = TableName.thriftToString(req.getAlter_table_params().getTable_name());
          break;
        case ALTER_VIEW:
          target = TableName.thriftToString(req.getAlter_view_params().getView_name());
          break;
        case CREATE_DATABASE:
          target = req.getCreate_db_params().getDb();
          break;
        case CREATE_TABLE_AS_SELECT:
        case CREATE_TABLE:
          target = TableName.thriftToString(req.getCreate_table_params().getTable_name());
          break;
        case CREATE_TABLE_LIKE:
          target = TableName.thriftToString(
              req.getCreate_table_like_params().getTable_name());
          break;
        case CREATE_VIEW:
          target = TableName.thriftToString(req.getCreate_view_params().getView_name());
          break;
        case CREATE_FUNCTION:
          target = FunctionName.thriftToString(
              req.getCreate_fn_params().getFn().getName());
          break;
        case COMMENT_ON: {
          TCommentOnParams params = req.getComment_on_params();
          if (params.isSetDb()) {
            target = "DB " + params.getDb();
          } else if (params.isSetTable_name()) {
            target = "TABLE " + TableName.thriftToString(params.getTable_name());
          } else if (params.isSetColumn_name()) {
            target = "COLUMN " + ColumnName.thriftToString(params.getColumn_name());
          } else {
            target = "";
          }
          break;
        }
        case DROP_STATS:
          target = TableName.thriftToString(req.getDrop_stats_params().getTable_name());
          break;
        case DROP_DATABASE:
          target = req.getDrop_db_params().getDb();
          break;
        case DROP_TABLE:
        case DROP_VIEW:
          target = TableName.thriftToString(
              req.getDrop_table_or_view_params().getTable_name());
          break;
        case TRUNCATE_TABLE:
          target = TableName.thriftToString(req.getTruncate_params().getTable_name());
          break;
        case DROP_FUNCTION:
          target = FunctionName.thriftToString(req.getDrop_fn_params().fn_name);
          break;
        case CREATE_ROLE:
        case DROP_ROLE:
          target = req.getCreate_drop_role_params().getRole_name();
          break;
        case GRANT_ROLE:
        case REVOKE_ROLE:
          target = req.getGrant_revoke_role_params().getRole_names() + " GROUP " +
              req.getGrant_revoke_role_params().getGroup_names();
          break;
        case GRANT_PRIVILEGE:
          target = "TO " + req.getGrant_revoke_priv_params().getPrincipal_name();
          break;
        case REVOKE_PRIVILEGE:
          target = "FROM " + req.getGrant_revoke_priv_params().getPrincipal_name();
          break;
        default:
          target = "";
      }
    } catch (Throwable t) {
      // This method is used for all DDL RPCs. We should not fail them by errors happen
      // here. Catch all exceptions and just log the error.
      target = "unknown target";
      LOG.error("Failed to get the target for request", t);
    }

    String user = "unknown user";
    if (req.isSetHeader() && req.header.isSetRequesting_user()) {
      user = req.header.requesting_user;
    }
    return String.format("%s%s issued by %s", req.ddl_type, " " + target, user);
  }

  /**
   * Get a short description for the resetMetadata request.
   */
  public static String getShortDescForReset(TResetMetadataRequest req) {
    String cmd = req.is_refresh ? "REFRESH " : "INVALIDATE ";
    if (req.isSetDb_name()) {
      if (req.is_refresh) cmd += "FUNCTIONS IN ";
      cmd += "DATABASE " + req.getDb_name();
    } else if (req.isSetTable_name()) {
      cmd += "TABLE " + TableName.fromThrift(req.getTable_name());
      if (req.isSetPartition_spec()) cmd += " PARTITIONS";
    } else if (req.isAuthorization()) {
      cmd += "AUTHORIZATION";
    } else {
      // Global INVALIDATE METADATA
      cmd += "ALL";
    }
    String user = req.header != null ? req.header.requesting_user : "unknown user";
    return String.format("%s issued by %s", cmd, user);
  }
}

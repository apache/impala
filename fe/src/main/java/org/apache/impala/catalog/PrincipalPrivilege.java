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

package org.apache.impala.catalog;

import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Represents a privilege that has been granted to a principal in an authorization policy.
 * This class is thread safe.
 */
public class PrincipalPrivilege extends CatalogObjectImpl {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);
  private static final String AUTHORIZABLE_SEPARATOR = "->";
  private static final String KV_SEPARATOR = "=";
  private final TPrivilege privilege_;
  private final String name_;

  private PrincipalPrivilege(TPrivilege privilege) {
    privilege_ = Preconditions.checkNotNull(privilege);
    name_ =  buildPrivilegeName(privilege_);
  }

  public TPrivilege toThrift() { return privilege_; }
  public static PrincipalPrivilege fromThrift(TPrivilege privilege) {
    return new PrincipalPrivilege(privilege);
  }

  /**
   * Builds a privilege name for the given TPrivilege object. For simplicity, this name is
   * generated in a format that can be sent to the Sentry client to perform authorization
   * checks. The format is:
   * [ServerName=value]->[DbName=value]->[TableName=value]->[ColumnName=value]->[Action Granted=value]->[Grant Option=value]
   */
  public static String buildPrivilegeName(TPrivilege privilege) {
    StringBuilder privilegeName = new StringBuilder();
    Preconditions.checkNotNull(privilege);
    TPrivilegeScope scope = privilege.getScope();
    Preconditions.checkNotNull(scope);
    switch (scope) {
      case SERVER: {
        privilegeName.append("server")
            .append(KV_SEPARATOR)
            .append(privilege.getServer_name().toLowerCase());
        break;
      }
      case URI: {
        privilegeName.append("server")
            .append(KV_SEPARATOR)
            .append(privilege.getServer_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        // (IMPALA-2695) URIs are case sensitive
        privilegeName.append("uri")
            .append(KV_SEPARATOR)
            .append(privilege.getUri());
        break;
      }
      case DATABASE: {
        privilegeName.append("server")
            .append(KV_SEPARATOR)
            .append(privilege.getServer_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("db")
            .append(KV_SEPARATOR)
            .append(privilege.getDb_name().toLowerCase());
        break;
      }
      case TABLE: {
        privilegeName.append("server")
            .append(KV_SEPARATOR)
            .append(privilege.getServer_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("db")
            .append(KV_SEPARATOR)
            .append(privilege.getDb_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("table")
            .append(KV_SEPARATOR)
            .append(privilege.getTable_name().toLowerCase());
        break;
      }
      case COLUMN: {
        privilegeName.append("server")
            .append(KV_SEPARATOR)
            .append(privilege.getServer_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("db")
            .append(KV_SEPARATOR)
            .append(privilege.getDb_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("table")
            .append(KV_SEPARATOR)
            .append(privilege.getTable_name().toLowerCase());
        privilegeName.append(AUTHORIZABLE_SEPARATOR);
        privilegeName.append("column")
            .append(KV_SEPARATOR)
            .append(privilege.getColumn_name().toLowerCase());
        break;
      }
      default: {
        throw new UnsupportedOperationException(
            "Unknown privilege scope: " + scope.toString());
      }
    }

    // The ALL privilege is always implied and does not need to be included as part
    // of the name.
    if (privilege.getPrivilege_level() != TPrivilegeLevel.ALL) {
      privilegeName.append(AUTHORIZABLE_SEPARATOR);
      privilegeName.append("action")
          .append(KV_SEPARATOR)
          .append(privilege.getPrivilege_level().toString().toLowerCase());
    }
    privilegeName.append(AUTHORIZABLE_SEPARATOR);
    privilegeName.append("grantoption")
        .append(KV_SEPARATOR)
        .append(privilege.isHas_grant_opt());
    return privilegeName.toString();
  }

  /**
   * Copies the given privilege and sets the has grant option.
   */
  public static TPrivilege copyPrivilegeWithGrant(TPrivilege privilege,
      boolean hasGrantOption) {
    TPrivilege copy = privilege.deepCopy();
    return copy.setHas_grant_opt(hasGrantOption);
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.PRIVILEGE;
  }
  @Override
  public String getName() { return name_; }
  public int getPrincipalId() { return privilege_.getPrincipal_id(); }
  public TPrincipalType getPrincipalType() { return privilege_.getPrincipal_type(); }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setPrivilege(toThrift());
  }

  // The time this principal was created. Used to quickly check if the same privilege
  // was dropped and re-created. Assumes a principal will not be created + dropped +
  // created in less than 1ms. Returns -1 if create_time_ms was not set for the privilege.
  public long getCreateTimeMs() {
    return privilege_.isSetCreate_time_ms() ? privilege_.getCreate_time_ms() : -1L;
  }
}

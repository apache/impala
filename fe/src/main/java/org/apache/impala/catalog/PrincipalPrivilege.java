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

import java.util.List;

import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a privilege that has been granted to a principal in an authorization policy.
 * This class is thread safe.
 */
public class PrincipalPrivilege extends CatalogObjectImpl {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);
  // These Joiners are used to build principal names. For simplicity, the principal name
  // we use can also be sent to the Sentry library to perform authorization checks
  // so we build them in the same format.
  private static final Joiner AUTHORIZABLE_JOINER = Joiner.on("->");
  private static final Joiner KV_JOINER = Joiner.on("=");
  private final TPrivilege privilege_;

  private PrincipalPrivilege(TPrivilege privilege) {
    privilege_ = Preconditions.checkNotNull(privilege);
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
    List<String> authorizable = Lists.newArrayListWithExpectedSize(4);
    try {
      Preconditions.checkNotNull(privilege);
      TPrivilegeScope scope = privilege.getScope();
      Preconditions.checkNotNull(scope);
      switch (scope) {
        case SERVER: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name().
              toLowerCase()));
          break;
        }
        case URI: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name().
              toLowerCase()));
          // (IMPALA-2695) URIs are case sensitive
          authorizable.add(KV_JOINER.join("uri", privilege.getUri()));
          break;
        }
        case DATABASE: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name().
              toLowerCase()));
          break;
        }
        case TABLE: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("table", privilege.getTable_name().
              toLowerCase()));
          break;
        }
        case COLUMN: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("table", privilege.getTable_name().
              toLowerCase()));
          authorizable.add(KV_JOINER.join("column", privilege.getColumn_name().
              toLowerCase()));
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
        authorizable.add(KV_JOINER.join("action",
            privilege.getPrivilege_level().toString()));
      }
      authorizable.add(KV_JOINER.join("grantoption", privilege.isHas_grant_opt()));
      return AUTHORIZABLE_JOINER.join(authorizable);
    } catch (Exception e) {
      // Should never make it here unless the privilege is malformed.
      LOG.error("ERROR: ", e);
      return null;
    }
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
  public String getName() { return buildPrivilegeName(privilege_); }
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

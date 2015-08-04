// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a privilege that has been granted to a role in an authorization policy.
 * This class is thread safe.
 */
public class RolePrivilege implements CatalogObject {
  private static final Logger LOG = Logger.getLogger(AuthorizationPolicy.class);
  // These Joiners are used to build role names. For simplicity, the role name we
  // use can also be sent to the Sentry library to perform authorization checks
  // so we build them in the same format.
  private static final Joiner AUTHORIZABLE_JOINER = Joiner.on("->");
  private static final Joiner KV_JOINER = Joiner.on("=");

  private final TPrivilege privilege_;
  private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

  private RolePrivilege(TPrivilege privilege) {
    privilege_ = privilege;
  }

  public TPrivilege toThrift() { return privilege_; }
  public static RolePrivilege fromThrift(TPrivilege privilege) {
    return new RolePrivilege(privilege);
  }

  /**
   * Builds a privilege name for the given TPrivilege object. For simplicity, this name is
   * generated in a format that can be sent to the Sentry client to perform authorization
   * checks.
   */
  public static String buildRolePrivilegeName(TPrivilege privilege) {
    List<String> authorizable = Lists.newArrayListWithExpectedSize(4);
    try {
      Preconditions.checkNotNull(privilege);
      TPrivilegeScope scope = privilege.getScope();
      Preconditions.checkNotNull(scope);
      switch (scope) {
        case SERVER: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name()));
          break;
        }
        case URI: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name()));
          authorizable.add(KV_JOINER.join("uri", privilege.getUri()));
          break;
        }
        case DATABASE: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name()));
          break;
        }
        case TABLE: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name()));
          authorizable.add(KV_JOINER.join("table", privilege.getTable_name()));
          break;
        }
        case COLUMN: {
          authorizable.add(KV_JOINER.join("server", privilege.getServer_name()));
          authorizable.add(KV_JOINER.join("db", privilege.getDb_name()));
          authorizable.add(KV_JOINER.join("table", privilege.getTable_name()));
          authorizable.add(KV_JOINER.join("column", privilege.getColumn_name()));
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
      return AUTHORIZABLE_JOINER.join(authorizable).toLowerCase();
    } catch (Exception e) {
      // Should never make it here unless the privilege is malformed.
      LOG.error("ERROR: ", e);
      return null;
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.PRIVILEGE;
  }
  @Override
  public String getName() { return privilege_.getPrivilege_name(); }
  public int getRoleId() { return privilege_.getRole_id(); }
  @Override
  public synchronized long getCatalogVersion() { return catalogVersion_; }
  @Override
  public synchronized void setCatalogVersion(long newVersion) {
    catalogVersion_ = newVersion;
  }
  @Override
  public boolean isLoaded() { return true; }

  // The time this role was created. Used to quickly check if the same privilege
  // was dropped and re-created. Assumes a role will not be created + dropped + created
  // in less than 1ms. Returns -1 if create_time_ms was not set for the privilege.
  public long getCreateTimeMs() {
    return privilege_.isSetCreate_time_ms() ? privilege_.getCreate_time_ms() : -1L;
  }
  public TPrivilegeScope getScope() { return privilege_.getScope(); }
}

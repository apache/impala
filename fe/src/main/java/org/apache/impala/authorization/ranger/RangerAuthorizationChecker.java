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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.impala.authorization.Authorizable;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.DefaultAuthorizableFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.common.InternalException;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link AuthorizationChecker} that uses Ranger.
 *
 * The Ranger implementation does not use catalog to cache the authorization policy.
 * Ranger plugin uses its own cache.
 */
public class RangerAuthorizationChecker extends AuthorizationChecker {
  public static final String UPDATE_ACCESS_TYPE = "update";
  public static final String REFRESH_ACCESS_TYPE = "read";
  private final RangerImpalaPlugin plugin_;

  public RangerAuthorizationChecker(AuthorizationConfig authzConfig) {
    super(authzConfig);
    Preconditions.checkArgument(authzConfig instanceof RangerAuthorizationConfig);
    RangerAuthorizationConfig rangerConfig = (RangerAuthorizationConfig) authzConfig;
    plugin_ = new RangerImpalaPlugin(
        rangerConfig.getServiceType(), rangerConfig.getAppId());
    plugin_.init();
  }

  @Override
  protected boolean authorize(User user, PrivilegeRequest request)
      throws InternalException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);
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
            .build());
        break;
      case TABLE:
        resources.add(new RangerImpalaResourceBuilder()
            .database(authorizable.getDbName())
            .table(authorizable.getTableName())
            .build());
        break;
      case COLUMN:
        RangerImpalaResourceBuilder builder = new RangerImpalaResourceBuilder();
        builder.database(authorizable.getDbName());
        // * in Ranger means "all". For example to check access for all columns, we need
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
        if (!authorize(plugin_, resource, user, request.getPrivilege())) {
          return false;
        }
      } else {
        boolean authorized = request.getPrivilege().hasAnyOf() ?
            authorizeAny(plugin_, resource, user,
                request.getPrivilege().getImpliedPrivileges()) :
            authorizeAll(plugin_, resource, user,
                request.getPrivilege().getImpliedPrivileges());
        if (!authorized) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public Set<String> getUserGroups(User user) throws InternalException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user.getShortName());
    return new HashSet<>(ugi.getGroups());
  }

  private boolean authorizeAny(RangerImpalaPlugin plugin,
      RangerAccessResourceImpl resource, User user, EnumSet<Privilege> privileges)
      throws InternalException {
    for (Privilege privilege: privileges) {
      if (authorize(plugin, resource, user, privilege)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeAll(RangerImpalaPlugin plugin,
      RangerAccessResourceImpl resource, User user, EnumSet<Privilege> privileges)
      throws InternalException {
    for (Privilege privilege: privileges) {
      if (!authorize(plugin, resource, user, privilege)) {
        return false;
      }
    }
    return true;
  }

  private boolean authorize(RangerImpalaPlugin plugin,
      RangerAccessResourceImpl resource, User user, Privilege privilege)
      throws InternalException {
    String accessType;
    if (privilege == Privilege.ANY) {
      accessType = RangerPolicyEngine.ANY_ACCESS;
    } else if (privilege == Privilege.INSERT) {
      // Ranger plugin for Hive considers INSERT to be UPDATE.
      accessType = UPDATE_ACCESS_TYPE;
    } else if (privilege == Privilege.REFRESH) {
      // TODO: this is a hack. It will need to be fixed once refresh is added into Hive
      // service definition.
      accessType = REFRESH_ACCESS_TYPE;
    } else {
      accessType = privilege.name().toLowerCase();
    }
    RangerAccessRequest request = new RangerAccessRequestImpl(resource,
        accessType, user.getShortName(), getUserGroups(user));
    RangerAccessResult result = plugin.isAccessAllowed(request);
    return result != null && result.getIsAllowed();
  }

  @VisibleForTesting
  public RangerImpalaPlugin getRangerImpalaPlugin() { return plugin_; }
}

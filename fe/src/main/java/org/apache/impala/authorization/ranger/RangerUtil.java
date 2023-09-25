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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TPrivilege;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Collection of static functions to support Apache Ranger implementation
 */
public class RangerUtil {
  private RangerUtil() { }

  /**
   * Creates a column resource for Ranger. Column resources also include
   * database and table information.
   */
  public static Map<String, String> createColumnResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.TABLE, getOrAll(privilege.getTable_name()));
    resource.put(RangerImpalaResourceBuilder.COLUMN,
        getOrAll(privilege.getColumn_name()));

    return resource;
  }

  /**
   * Creates a URI resource for Ranger. In Ranger a URI is known as a URL.
   */
  public static Map<String, String> createUriResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();
    String uri = privilege.getUri();
    resource.put(RangerImpalaResourceBuilder.URL, uri == null ? "*" : uri);

    return resource;
  }

  /**
   * Creates a function resource for Ranger. Function resources also include
   * database information.
   */
  public static Map<String, String> createFunctionResource(TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.DATABASE, getOrAll(privilege.getDb_name()));
    resource.put(RangerImpalaResourceBuilder.UDF, getOrAll(privilege.getFn_name()));

    return resource;
  }

  public static Map<String, String> createStorageHandlerUriResource(
      TPrivilege privilege) {
    Map<String, String> resource = new HashMap<>();

    resource.put(RangerImpalaResourceBuilder.STORAGE_TYPE,
        getOrAll(privilege.getStorage_type()));
    resource.put(RangerImpalaResourceBuilder.STORAGE_URL,
        getOrAll(privilege.getStorage_url()));

    return resource;
  }

  private static String getOrAll(String resource) {
    return (resource == null) ? "*" : resource;
  }

  /**
   * This method returns the groups that 'user' belongs to. By starting impalad and
   * catalogd with the argument of "use_customized_user_groups_mapper_for_ranger",
   * the customized user-to-groups mapper would be provided, which is useful in the
   * testing environment.
   */
  public static Set<String> getGroups(String user) {
    UserGroupInformation ugi;
    if (RuntimeEnv.INSTANCE.isTestEnv() ||
        BackendConfig.INSTANCE.useCustomizedUserGroupsMapperForRanger()) {
      ugi = UserGroupInformation.createUserForTesting(user,
          new String[]{user});
    } else {
      ugi = UserGroupInformation.createRemoteUser(user);
    }
    return Sets.newHashSet(ugi.getGroupNames());
  }

  /**
   * For now there is no dedicated REST API that allows Impala to determine whether
   * 'user' is a Ranger administrator. In this regard, we call
   * RangerBasePlugin#getAllRoles(), whose server side method,
   * RoleREST#getAllRoleNames(), will call RoleREST#ensureAdminAccess() on 'user' to make
   * sure 'user' is a Ranger administrator. An Exception will be thrown if it is not the
   * case.
   * RANGER-3127 has been created to keep track of the issue.
   */
  public static void validateRangerAdmin(RangerImpalaPlugin plugin, String user)
      throws Exception {
    plugin.getAllRoles(user, null);
  }

  public static boolean roleExists(RangerImpalaPlugin plugin, String roleName) {
    Set<RangerRole> roleSet = plugin.getRoles().getRangerRoles();
    if (roleSet == null) return false;
    return roleSet.stream().anyMatch(r -> r.getName().equals(roleName));
  }
}

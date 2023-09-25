/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.authorization;

import org.apache.impala.authorization.ranger.RangerAuthorizationChecker;
import org.apache.impala.authorization.ranger.RangerImpalaPlugin;
import org.apache.impala.authorization.ranger.RangerUtil;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.Role;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CatalogServiceTestCatalogWithRanger extends CatalogServiceTestCatalog {
  private static final String RANGER_ADMIN_USER = "admin";
  private RangerImpalaPlugin rangerImpalaPlugin_;
  protected CatalogServiceTestCatalogWithRanger(boolean loadInBackground,
      int numLoadingThreads, MetaStoreClientPool metaStoreClientPool)
      throws ImpalaException {
    super(loadInBackground, numLoadingThreads, metaStoreClientPool);
  }

  public static CatalogServiceTestCatalog create() {
    return createWithAuth(new NoopAuthorizationFactory());
  }

  public static CatalogServiceTestCatalog createWithAuth(AuthorizationFactory factory) {
    return createWithAuth(factory, new BaseTestCatalogSupplier() {
      @Override
      public CatalogServiceTestCatalog get() throws ImpalaException {
        CatalogServiceTestCatalogWithRanger cs;
        MetaStoreClientPool metaStoreClientPool = new MetaStoreClientPool(0, 0);
        cs = new CatalogServiceTestCatalogWithRanger(false, 16, metaStoreClientPool);
        RangerImpalaPlugin rangerImpalaPlugin =
            ((RangerAuthorizationChecker) factory.newAuthorizationChecker())
                .getRangerImpalaPlugin();
        cs.setRangerImpalaPlugin(rangerImpalaPlugin);
        return cs;
      }
    });
  }

  public void setRangerImpalaPlugin(RangerImpalaPlugin rangerImpalaPlugin_) {
    this.rangerImpalaPlugin_ = rangerImpalaPlugin_;
  }

  @Override
  public Role addRole(String roleName, Set<String> grantGroups) {
    Role authRole = null;
    RangerRole role = new RangerRole();
    role.setName(roleName);
    role.setCreatedByUser(RANGER_ADMIN_USER);
    List<RangerRole.RoleMember> roleMemberList =
        grantGroups.stream()
            .map(s -> new RangerRole.RoleMember(s, s.equals(RANGER_ADMIN_USER)))
            .collect(Collectors.toList());
    role.setGroups(roleMemberList);
    try {
      rangerImpalaPlugin_.createRole(role, null);
      rangerImpalaPlugin_.refreshPoliciesAndTags();
      authRole = super.addRole(roleName, grantGroups);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return authRole;
  }

  @Override
  public Role removeRole(String roleName) {
    Role authRole = null;
    try {
      RangerUtil.validateRangerAdmin(rangerImpalaPlugin_, RANGER_ADMIN_USER);
      rangerImpalaPlugin_.dropRole(RANGER_ADMIN_USER, roleName, null);
      // need to invoke plugin to sync policy to avoid stale roles
      // still exists in plugin cache
      rangerImpalaPlugin_.refreshPoliciesAndTags();
      authRole = super.removeRole(roleName);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return authRole;
  }
}

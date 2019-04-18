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

package org.apache.impala.authorization.sentry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TPrincipalType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.authorization.sentry.SentryProxy.AuthorizationDelta;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SentryProxyTest {
  private final String PRINCIPAL_NAME_PREFIX = "sentry_proxy";
  private static final String SENTRY_SERVER = "server1";
  private static final org.apache.impala.authorization.User USER =
      new org.apache.impala.authorization.User(System.getProperty("user.name"));
  private final SentryPolicyService sentryService_;
  private final SentryAuthorizationConfig authzConfig_;

  public SentryProxyTest() {
    authzConfig_ = SentryAuthorizationConfig.createHadoopGroupAuthConfig(
        SENTRY_SERVER,
        System.getenv("IMPALA_HOME") + "/fe/src/test/resources/sentry-site.xml");
    sentryService_ = new SentryPolicyService(authzConfig_.getSentryConfig());
  }

  @Before
  public void setUp() throws ImpalaException {
    cleanUpRoles();
  }

  @After
  public void cleanUp() throws ImpalaException {
    cleanUpRoles();
  }

  private void cleanUpRoles() throws ImpalaException {
    for (TSentryRole role: sentryService_.listAllRoles(USER)) {
      if (role.getRoleName().startsWith(PRINCIPAL_NAME_PREFIX)) {
        sentryService_.dropRole(USER, role.getRoleName(), true);
      }
    }
  }

  @Test
  public void testEmptyToNonEmptyCatalog() {
    withAllPrincipalTypes(ctx -> {
      String[] principalNames = new String[3];
      for (int i = 0; i < principalNames.length; i++) {
        principalNames[i] = String.format("%s_add_%d", PRINCIPAL_NAME_PREFIX, i);
      }

      for (String principalName: principalNames) {
        addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName,
            "functional");
      }

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      for (String principalName: principalNames) {
        checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName,
            "server=server1->db=functional->grantoption=false");
      }

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testNonEmptyToEmptyCatalog() {
    withAllPrincipalTypes(ctx -> {
      String[] principalNames = new String[3];
      for (int i = 0; i < principalNames.length; i++) {
        principalNames[i] = String.format("%s_add_%d", PRINCIPAL_NAME_PREFIX, i);
      }

      for (String principalName: principalNames) {
        addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName,
            "functional");
      }

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      for (String principalName: principalNames) {
        checkNoCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName);
      }

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testAddCatalog() {
    withAllPrincipalTypes(ctx -> {
      String principalName = String.format("%s_add", PRINCIPAL_NAME_PREFIX);
      addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName, "functional");
      addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName,
          "functional");

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName,
          "server=server1->db=functional->grantoption=false");

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testUpdateCatalog() {
    withAllPrincipalTypes(ctx -> {
      String principalName = String.format("%s_update", PRINCIPAL_NAME_PREFIX);
      addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName, "functional");
      addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName,
          "functional", "functional_kudu");

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName,
          "server=server1->db=functional->grantoption=false",
          "server=server1->db=functional_kudu->grantoption=false");

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testDropCatalog() {
    withAllPrincipalTypes(ctx -> {
      String principalName = String.format("%s_remove", PRINCIPAL_NAME_PREFIX);
      addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName, "functional");
      dropSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName);

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkNoCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName);

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testMergeCatalog() {
    withAllPrincipalTypes(ctx -> {
      String addPrincipal = String.format("%s_add", PRINCIPAL_NAME_PREFIX);
      String updatePrincipal = String.format("%s_update", PRINCIPAL_NAME_PREFIX);
      String removePrincipal = String.format("%s_remove", PRINCIPAL_NAME_PREFIX);

      addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, updatePrincipal,
          "functional");
      addCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, removePrincipal,
          "functional");

      addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, addPrincipal,
          "functional");
      addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, updatePrincipal,
          "functional", "functional_kudu");
      dropSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, removePrincipal);

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, addPrincipal,
          "server=server1->db=functional->grantoption=false");
      checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, updatePrincipal,
          "server=server1->db=functional->grantoption=false",
          "server=server1->db=functional_kudu->grantoption=false");
      checkNoCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, removePrincipal);

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testUpdateSentryNotInCatalog() {
    withAllPrincipalTypes(ctx -> {
      String principalName = String.format("%s_update", PRINCIPAL_NAME_PREFIX);
      addSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName,
          "functional", "functional_kudu");

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName,
          "server=server1->db=functional->grantoption=false",
          "server=server1->db=functional_kudu->grantoption=false");

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testDropSentryNotInCatalog() {
    withAllPrincipalTypes(ctx -> {
      String principalName = String.format("%s_remove", PRINCIPAL_NAME_PREFIX);
      dropSentryPrincipalPrivileges(ctx.type_, ctx.sentryService_, principalName);

      CatalogState noReset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          false);

      checkNoCatalogPrincipalPrivileges(ctx.type_, ctx.catalog_, principalName);

      CatalogState reset = refreshSentryAuthorization(ctx.catalog_, ctx.sentryService_,
          true);
      checkCatalogState(noReset, reset);
    });
  }

  @Test
  public void testRoleNameCaseInsensitive() throws ImpalaException {
    String lowerCaseRoleName = String.format("%s_case_insensitive_role",
        PRINCIPAL_NAME_PREFIX);
    String upperCaseRoleName = lowerCaseRoleName.toUpperCase();
    String mixedCaseRoleName = lowerCaseRoleName.substring(0, 1).toUpperCase() +
        lowerCaseRoleName.substring(1);

    try (CatalogServiceCatalog catalog = CatalogServiceTestCatalog.createWithAuth(
        new SentryAuthorizationFactory(authzConfig_))) {
      addSentryRolePrivileges(sentryService_, lowerCaseRoleName, "functional");
      CatalogState noReset = refreshSentryAuthorization(catalog, sentryService_, false);

      // Impala stores the role name in case insensitive way.
      for (String roleName : new String[]{
          lowerCaseRoleName, upperCaseRoleName, mixedCaseRoleName}) {
        Role role = catalog.getAuthPolicy().getRole(roleName);
        assertEquals(lowerCaseRoleName, role.getName());
        assertEquals(1, role.getPrivileges().size());
        assertNotNull(role.getPrivilege(
            "server=server1->db=functional->grantoption=false"));
      }

      try {
        sentryService_.createRole(USER, upperCaseRoleName, false);
        fail("Exception should be thrown when creating a duplicate role name.");
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Error creating role"));
      }

      // No new role should be added.
      for (String roleName : new String[]{
          lowerCaseRoleName, upperCaseRoleName, mixedCaseRoleName}) {
        Role role = catalog.getAuthPolicy().getRole(roleName);
        assertEquals(lowerCaseRoleName, role.getName());
        assertEquals(1, role.getPrivileges().size());
        assertNotNull(role.getPrivilege(
            "server=server1->db=functional->grantoption=false"));
      }

      CatalogState reset = refreshSentryAuthorization(catalog, sentryService_, true);
      checkCatalogState(noReset, reset);
    }
  }

  @Test
  public void testUserNameCaseSensitive() throws ImpalaException {
    String lowerCaseUserName = String.format("%s_case_sensitive_user",
        PRINCIPAL_NAME_PREFIX);
    String upperCaseUserName = lowerCaseUserName.toUpperCase();
    String mixedCaseUserName = lowerCaseUserName.substring(0, 1).toUpperCase() +
        lowerCaseUserName.substring(1);

    try (CatalogServiceCatalog catalog = CatalogServiceTestCatalog.createWithAuth(
        new SentryAuthorizationFactory(authzConfig_))) {
      SentryPolicyServiceStub sentryService = createSentryPolicyServiceStub(
          authzConfig_.getSentryConfig());
      // We grant different privileges to different users to ensure each user is
      // granted with a distinct privilege.
      addSentryUserPrivileges(sentryService, lowerCaseUserName, "functional");
      addSentryUserPrivileges(sentryService, upperCaseUserName, "functional_kudu");
      addSentryUserPrivileges(sentryService, mixedCaseUserName, "functional_parquet");

      CatalogState noReset = refreshSentryAuthorization(catalog, sentryService, false);

      // Impala stores the user name in case sensitive way.
      for (Pair<String, String> userPrivilege : new Pair[]{
          new Pair(lowerCaseUserName, "server=server1->db=functional->grantoption=false"),
          new Pair(upperCaseUserName, "server=server1->db=functional_kudu" +
              "->grantoption=false"),
          new Pair(mixedCaseUserName, "server=server1->db=functional_parquet" +
              "->grantoption=false")}) {
        User user = catalog.getAuthPolicy().getUser(userPrivilege.first);
        assertEquals(userPrivilege.first, user.getName());
        assertEquals(1, user.getPrivileges().size());
        assertNotNull(user.getPrivilege(userPrivilege.second));
      }

      CatalogState reset = refreshSentryAuthorization(catalog, sentryService, true);
      checkCatalogState(noReset, reset);
    }
  }

  private static void addCatalogPrincipalPrivileges(TPrincipalType type,
      CatalogServiceCatalog catalog, String principalName, String... dbNames) {
    try {
      switch (type) {
        case ROLE:
          addCatalogRolePrivileges(catalog, principalName, dbNames);
          break;
        case USER:
          addCatalogUserPrivileges(catalog, principalName, dbNames);
          break;
      }
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  private static void addCatalogRolePrivileges(CatalogServiceCatalog catalog,
      String roleName, String... dbNames) throws ImpalaException {
    Role role = catalog.addRole(roleName, Sets.newHashSet(USER.getShortName()));
    for (String dbName: dbNames) {
      catalog.addRolePrivilege(roleName, createRolePrivilege(role.getId(), dbName));
    }
  }

  private static void addCatalogUserPrivileges(CatalogServiceCatalog catalog,
      String userName, String... dbNames) throws ImpalaException {
    User user = catalog.addUser(userName);
    for (String dbName: dbNames) {
      catalog.addUserPrivilege(userName, createUserPrivilege(user.getId(), dbName));
    }
  }

  private static void addSentryPrincipalPrivileges(TPrincipalType type,
      SentryPolicyService sentryService, String principalName, String... dbNames) {
    try {
      switch (type) {
        case ROLE:
          addSentryRolePrivileges(sentryService, principalName, dbNames);
          break;
        case USER:
          addSentryUserPrivileges(sentryService, principalName, dbNames);
          break;
      }
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  private static void addSentryRolePrivileges(SentryPolicyService sentryService,
      String roleName, String... dbNames) throws ImpalaException {
    sentryService.createRole(USER, roleName, false);
    for (String dbName: dbNames) {
      sentryService.grantRolePrivilege(USER, roleName, createRolePrivilege(dbName));
    }
  }

  private static void addSentryUserPrivileges(SentryPolicyService sentryService,
      String userName, String... dbNames) throws ImpalaException {
    Preconditions.checkArgument(sentryService instanceof SentryPolicyServiceStub);
    SentryPolicyServiceStub stub = (SentryPolicyServiceStub) sentryService;
    stub.createUser(userName);
    for (String dbName: dbNames) {
      stub.grantUserPrivilege(userName, createUserPrivilege(dbName));
    }
  }

  private static void dropSentryPrincipalPrivileges(TPrincipalType type,
      SentryPolicyService sentryService, String principalName) {
    try {
      switch (type) {
        case ROLE:
          dropSentryRolePrivileges(sentryService, principalName);
          break;
        case USER:
          dropSentryUserPrivileges(sentryService, principalName);
          break;
      }
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  private static void dropSentryRolePrivileges(SentryPolicyService sentryService,
      String roleName) throws ImpalaException {
    sentryService.dropRole(USER, roleName, true);
  }

  private static void dropSentryUserPrivileges(SentryPolicyService sentryService,
      String userName) throws ImpalaException {
    Preconditions.checkArgument(sentryService instanceof SentryPolicyServiceStub);
    SentryPolicyServiceStub stub = (SentryPolicyServiceStub) sentryService;
    stub.dropRole(USER, userName, true);
  }

  private static void checkCatalogPrincipalPrivileges(TPrincipalType type,
      CatalogServiceCatalog catalog, String principalName, String... privileges) {
    switch (type) {
      case ROLE:
        checkCatalogRolePrivileges(catalog, principalName, privileges);
        break;
      case USER:
        checkCatalogUserPrivileges(catalog, principalName, privileges);
        break;
    }
  }

  private static void checkCatalogRolePrivileges(CatalogServiceCatalog catalog,
      String roleName, String... privileges) {
    Role role = catalog.getAuthPolicy().getRole(roleName);
    assertEquals(role.getName(), roleName);
    assertEquals(privileges.length, role.getPrivileges().size());
    for (String privilege: privileges) {
      assertNotNull(role.getPrivilege(privilege));
    }
  }

  private static void checkCatalogUserPrivileges(CatalogServiceCatalog catalog,
      String userName, String... privileges) {
    User user = catalog.getAuthPolicy().getUser(userName);
    assertEquals(user.getName(), userName);
    assertEquals(privileges.length, user.getPrivileges().size());
    for (String privilege: privileges) {
      assertNotNull(user.getPrivilege(privilege));
    }
  }

  private static void checkNoCatalogPrincipalPrivileges(TPrincipalType type,
      CatalogServiceCatalog catalog, String principalName) {
    switch (type) {
      case ROLE:
        assertNull(catalog.getAuthPolicy().getRole(principalName));
        break;
      case USER:
        assertNull(catalog.getAuthPolicy().getUser(principalName));
        break;
    }
  }

  private static long getAuthCatalogSize(CatalogServiceCatalog catalog) {
    return catalog.getAuthPolicy().getAllRoles().size() +
        catalog.getAuthPolicy().getAllRoles().stream()
            .mapToInt(p -> p.getPrivileges().size()).sum() +
        catalog.getAuthPolicy().getAllUsers().size() +
        catalog.getAuthPolicy().getAllUsers().stream()
            .mapToInt(p -> p.getPrivileges().size()).sum();
  }

  private static void checkCatalogState(CatalogState noReset, CatalogState reset) {
    // Resetting the version means the new version will be current version + the number
    // of all authorization catalog objects.
    assertEquals(noReset.catalogVersion_ + noReset.catalogSize_, reset.catalogVersion_);
    // Catalog size should be the same regardless whether or not the versions are reset.
    assertEquals(noReset.catalogSize_, reset.catalogSize_);
  }

  private static class CatalogState {
    private final long catalogVersion_;
    private final long catalogSize_;

    public CatalogState(long catalogVersion, long catalogSize) {
      catalogVersion_ = catalogVersion;
      catalogSize_ = catalogSize;
    }
  }

  private static CatalogState refreshSentryAuthorization(CatalogServiceCatalog catalog,
      SentryPolicyService sentryService, boolean resetVersions) {
    SentryProxy.refreshSentryAuthorization(catalog, sentryService, USER, resetVersions,
        false, new AuthorizationDelta());
    return new CatalogState(catalog.getCatalogVersion(), getAuthCatalogSize(catalog));
  }

  private static class SentryPolicyServiceStub extends SentryPolicyService {
    private final Map<String, Set<TSentryPrivilege>> allUserPrivileges_ =
        Maps.newHashMap();

    public SentryPolicyServiceStub(SentryConfig sentryConfig) {
      super(sentryConfig);
    }

    @Override
    public Map<String, Set<TSentryPrivilege>> listAllUsersPrivileges(
        org.apache.impala.authorization.User requestingUser) throws ImpalaException {
      return allUserPrivileges_;
    }

    public void createUser(String userName) {
      allUserPrivileges_.put(userName, Sets.newHashSet());
    }

    public void grantUserPrivilege(String userName, TPrivilege privilege) {
      allUserPrivileges_.get(userName).add(createSentryPrivilege(privilege.getDb_name()));
    }

    public void dropUser(String userName) {
      allUserPrivileges_.remove(userName);
    }
  }

  private static SentryPolicyServiceStub createSentryPolicyServiceStub(
      SentryConfig sentryConfig) {
    return new SentryPolicyServiceStub(sentryConfig);
  }

  private static TPrivilege createRolePrivilege(int roleId, String dbName) {
    return createPrincipalPrivilege(TPrincipalType.ROLE, roleId, dbName);
  }

  private static TPrivilege createRolePrivilege(String dbName) {
    return createRolePrivilege(0, dbName);
  }

  private static TPrivilege createUserPrivilege(int userId, String dbName) {
    return createPrincipalPrivilege(TPrincipalType.USER, userId, dbName);
  }

  private static TPrivilege createUserPrivilege(String dbName) {
    return createPrincipalPrivilege(TPrincipalType.USER, 0, dbName);
  }

  private static TPrivilege createPrincipalPrivilege(TPrincipalType principalType,
      int principalId, String dbName) {
    TPrivilege privilege = new TPrivilege(TPrivilegeLevel.ALL, TPrivilegeScope.DATABASE,
        false);
    privilege.setPrincipal_id(principalId);
    privilege.setPrincipal_type(principalType);
    privilege.setServer_name("server1");
    privilege.setDb_name(dbName);
    return privilege;
  }

  private static TSentryPrivilege createSentryPrivilege(String dbName) {
    TSentryPrivilege privilege = new TSentryPrivilege("DATABASE", "server1", "*");
    privilege.setDbName(dbName);
    return privilege;
  }

  private static class SentryProxyTestContext {
    private final TPrincipalType type_;
    private final CatalogServiceCatalog catalog_;
    private final SentryPolicyService sentryService_;

    public SentryProxyTestContext(TPrincipalType type, CatalogServiceCatalog catalog,
        SentryPolicyService sentryService) {
      type_ = type;
      catalog_ = catalog;
      sentryService_ = sentryService;
    }
  }

  private void withAllPrincipalTypes(Consumer<SentryProxyTestContext> consumer) {
    for (TPrincipalType type: TPrincipalType.values()) {
      try (CatalogServiceCatalog catalog = CatalogServiceTestCatalog.createWithAuth(
          new SentryAuthorizationFactory(authzConfig_))) {
        SentryPolicyService sentryService = sentryService_;
        if (type == TPrincipalType.USER) {
          sentryService = createSentryPolicyServiceStub(authzConfig_.getSentryConfig());
        }
        consumer.accept(new SentryProxyTestContext(type, catalog, sentryService));
      }
    }
  }
}
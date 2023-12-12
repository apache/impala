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

package org.apache.impala.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.authorization.ranger.RangerAuthorizationChecker;
import org.apache.impala.authorization.ranger.RangerAuthorizationConfig;
import org.apache.impala.authorization.ranger.RangerAuthorizationFactory;
import org.apache.impala.authorization.ranger.RangerCatalogdAuthorizationManager;
import org.apache.impala.authorization.ranger.RangerImpalaPlugin;
import org.apache.impala.authorization.ranger.RangerImpalaResourceBuilder;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TDescribeTableParams;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TTableName;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status.Family;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base class for authorization tests.
 */
public abstract class AuthorizationTestBase extends FrontendTestBase {
  public static final Logger LOG = LoggerFactory.getLogger(AuthorizationTestBase.class);
  protected static final String RANGER_ADMIN_URL = "http://localhost:6080";
  protected static final String RANGER_USER = "admin";
  protected static final String RANGER_PASSWORD = "admin";
  protected static final String SERVER_NAME = "server1";

  // For the Ranger tests, 'OWNER_USER' is used to denote a requesting user that is
  // the owner of the resource.
  protected static final User OWNER_USER = new User(System.getProperty("user.name"));
  // For the Ranger tests, 'GROUPS' is used to denote the name of the group where a
  // non-owner is associated with.
  protected static final List<String> GROUPS = Collections.singletonList("non_owner");
  // For the Ranger tests, 'OWNER_GROUPS' is used to denote the name of the group where
  // an owner is associated with.
  protected static final List<String> OWNER_GROUPS =
      Collections.singletonList(System.getProperty("user.name"));

  protected static final String RANGER_SERVICE_TYPE = "hive";
  protected static final String RANGER_SERVICE_NAME = "test_impala";
  protected static final String RANGER_APP_ID = "impala";
  protected static final User RANGER_ADMIN = new User("admin");

  // For the Ranger tests, 'user_' is used to denote a requesting user that is not the
  // owner of the resource. Note that we defer the setup of 'user_' to the constructor
  // and assign a different name for each authorization provider to avoid the need for
  // providing a customized group mapping service when instantiating a Sentry
  // ResourceAuthorizationProvider as we do in TestShortUsernameWithAuthToLocal() of
  // AuthorizationTest.java.
  protected static User user_ = null;
  // For the Ranger tests, 'as_owner_' is used to indicate whether or not the requesting
  // user in a test query is the owner of the resource.
  protected static boolean as_owner_ = false;

  protected final AuthorizationConfig authzConfig_;
  protected final AuthorizationFactory authzFactory_;
  protected final AuthorizationProvider authzProvider_;
  protected final AnalysisContext authzCtx_;
  protected final ImpaladTestCatalog authzCatalog_;
  protected final Frontend authzFrontend_;
  protected final RangerImpalaPlugin rangerImpalaPlugin_;
  protected final RangerRESTClient rangerRestClient_;

  protected final CatalogServiceTestCatalog testCatalog_;

  public AuthorizationTestBase(AuthorizationProvider authzProvider)
      throws ImpalaException {
    authzProvider_ = authzProvider;
    switch (authzProvider) {
      case RANGER:
        user_ = new User("non_owner");
        authzConfig_ = new RangerAuthorizationConfig(RANGER_SERVICE_TYPE, RANGER_APP_ID,
            SERVER_NAME, null, null, null);
        authzFactory_ = createAuthorizationFactory(authzProvider);
        authzCtx_ = createAnalysisCtx(authzFactory_, user_.getName());
        testCatalog_ = CatalogServiceTestCatalogWithRanger.createWithAuth(authzFactory_);
        authzCatalog_ = new ImpaladTestCatalog(testCatalog_);
        authzFrontend_ = new Frontend(authzFactory_, authzCatalog_);
        rangerImpalaPlugin_ =
            ((RangerAuthorizationChecker) authzFrontend_.getAuthzChecker())
                .getRangerImpalaPlugin();
        assertEquals("test-cluster", rangerImpalaPlugin_.getClusterName());
        rangerRestClient_ = new RangerRESTClient(RANGER_ADMIN_URL, null,
            rangerImpalaPlugin_.getConfig());
        rangerRestClient_.setBasicAuthInfo(RANGER_USER, RANGER_PASSWORD);
        break;
      default:
        throw new IllegalArgumentException(String.format(
            "Unsupported authorization provider: %s", authzProvider));
    }
  }

  protected AuthorizationFactory createAuthorizationFactory(
      AuthorizationProvider authzProvider) {
    return new RangerAuthorizationFactory(authzConfig_);
  }

  protected interface WithPrincipal {
    void init(TPrivilege[]... privileges) throws ImpalaException;
    void cleanUp() throws ImpalaException;
    String getName();
  }

  protected abstract class WithRanger implements WithPrincipal {
    private final List<GrantRevokeRequest> grants = new ArrayList<>();
    private final RangerCatalogdAuthorizationManager authzManager =
        new RangerCatalogdAuthorizationManager(() -> rangerImpalaPlugin_, null);

    @Override
    public void init(TPrivilege[]... privileges) throws ImpalaException {
      for (TPrivilege[] privilege : privileges) {
        grants.addAll(buildRequest(Arrays.asList(privilege))
            .stream()
            .peek(r -> {
              r.setResource(updateUri(r.getResource()));
              if (r.getAccessTypes().contains("owner")) {
                r.getAccessTypes().remove("owner");
                r.getAccessTypes().add("all");
              }
            }).collect(Collectors.toList()));
      }

      authzManager.grantPrivilege(grants, "", "127.0.0.1");
      rangerImpalaPlugin_.refreshPoliciesAndTags();
    }

    /**
     * Create the {@link GrantRevokeRequest}s used for granting and revoking privileges.
     */
    protected abstract List<GrantRevokeRequest> buildRequest(List<TPrivilege> privileges);

    @Override
    public void cleanUp() throws ImpalaException {
      authzManager.revokePrivilege(grants, "", "127.0.0.1");
    }

    /**
     * Depending on whether or not the principal is the owner of the resource, we return
     * either 'OWNER_USER' or 'user_'. This function is used in authzOk() and
     * anthzError().
     */
    @Override
    public String getName() {
      return (as_owner_ ? OWNER_USER.getName() : user_.getName());
    }
  }

  public class WithRangerUser extends WithRanger {
    @Override
    protected List<GrantRevokeRequest> buildRequest(List<TPrivilege> privileges) {
      return RangerCatalogdAuthorizationManager.createGrantRevokeRequests(
          RANGER_ADMIN.getName(), true,
          // We provide the name of the grantee, which is a user in this case, according
          // to whether or not we test the query with the requesting user that is the
          // owner of the resource.
          getName(),
          Collections.emptyList(), Collections.emptyList(),
          rangerImpalaPlugin_.getClusterName(), "127.0.0.1", privileges,
          /*resourceOwner*/ null);
    }
  }

  public class WithRangerGroup extends WithRanger {
    @Override
    protected List<GrantRevokeRequest> buildRequest(List<TPrivilege> privileges) {
      return RangerCatalogdAuthorizationManager.createGrantRevokeRequests(
          RANGER_ADMIN.getName(), true, null,
          // We provide the name of the grantee, which is a group in this case, according
          // to whether or not we test the query with the requesting user that is the
          // owner of the resource.
          (as_owner_ ? OWNER_GROUPS : GROUPS), Collections.emptyList(),
          // groups,
          rangerImpalaPlugin_.getClusterName(), "127.0.0.1", privileges,
          /*resourceOwner*/ null);
    }
  }

  private static Map<String, String> updateUri(Map<String, String> resources) {
    String uri = resources.get(RangerImpalaResourceBuilder.URL);
    if (uri != null && uri.startsWith("/")) {
      uri = "hdfs://localhost:20500" + uri;
    }
    resources.put(RangerImpalaResourceBuilder.URL, uri);

    return resources;
  }

  protected class DescribeOutput {
    private String[] excludedStrings_ = new String[0];
    private String[] includedStrings_ = new String[0];
    private final TDescribeOutputStyle outputStyle_;

    public DescribeOutput(TDescribeOutputStyle style) {
      outputStyle_ = style;
    }

    /**
     * Indicates which strings must not appear in the output of the describe statement.
     * During validation, if one of these strings exists, an assertion is thrown.
     *
     * @param excluded - Array of strings that must not exist in the output.
     * @return DescribeOutput instance.
     */
    public DescribeOutput excludeStrings(String[] excluded) {
      excludedStrings_ = excluded;
      return this;
    }

    /**
     * Indicates which strings are required to appear in the output of the describe
     * statement.  During validation, if any one of these strings does not exist, an
     * assertion is thrown.
     *
     * @param included - Array of strings that must exist in the output.
     * @return DescribeOutput instance.
     */
    public DescribeOutput includeStrings(String[] included) {
      includedStrings_ = included;
      return this;
    }

    public void validate(TTableName table) throws ImpalaException {
      Preconditions.checkArgument(includedStrings_.length != 0 ||
              excludedStrings_.length != 0,
          "One or both of included or excluded strings must be defined.");
      TDescribeTableParams testParams = new TDescribeTableParams();
      testParams.setTable_name(table);
      testParams.setOutput_style(outputStyle_);
      List<String> result = resultToStringList(authzFrontend_.describeTable(testParams,
          user_));
      for (String str: includedStrings_) {
        assertTrue(String.format("\"%s\" is not in the describe output.\n" +
                "Expected : %s\n Actual   : %s", str, Arrays.toString(includedStrings_),
            result), result.contains(str));
      }
      for (String str: excludedStrings_) {
        assertTrue(String.format("\"%s\" should not be in the describe output.", str),
            !result.contains(str));
      }
    }
  }

  protected DescribeOutput describeOutput(TDescribeOutputStyle style) {
    return new DescribeOutput(style);
  }

  protected List<WithPrincipal> buildWithPrincipals() {
    List<WithPrincipal> withPrincipals = new ArrayList<>();
    switch (authzProvider_) {
      case RANGER:
        withPrincipals.add(new WithRangerUser());
        withPrincipals.add(new WithRangerGroup());
        break;
      default:
        throw new IllegalArgumentException(String.format(
            "Unsupported authorization provider: %s", authzProvider_));
    }
    return withPrincipals;
  }

  protected class AuthzTest {
    private final AnalysisContext context_;
    private final String stmt_;

    public AuthzTest(String stmt) {
      this(null, stmt);
    }

    public AuthzTest(AnalysisContext context, String stmt) {
      Preconditions.checkNotNull(stmt);
      context_ = context;
      stmt_ = stmt;
    }

    /**
     * This method runs with the specified privileges.
     */
    public AuthzTest ok(TPrivilege[]... privileges)
        throws ImpalaException {
      ok(/* expectAnalysisOk */ true, privileges);
      return this;
    }

    /**
     * This method runs with the specified privileges.
     */
    public AuthzTest ok(boolean expectAnalysisOk, TPrivilege[]... privileges)
        throws ImpalaException {
      for (WithPrincipal withPrincipal: buildWithPrincipals()) {
        try {
          withPrincipal.init(privileges);
          if (context_ != null) {
            authzOk(context_, stmt_, withPrincipal, expectAnalysisOk);
          } else {
            authzOk(stmt_, withPrincipal, expectAnalysisOk);
          }
        } finally {
          withPrincipal.cleanUp();
        }
      }
      return this;
    }

    /**
     * This method runs with the specified privileges.
     */
    public AuthzTest okDescribe(TTableName table, DescribeOutput output,
        TPrivilege[]... privileges) throws ImpalaException {
      for (WithPrincipal withPrincipal: buildWithPrincipals()) {
        try {
          withPrincipal.init(privileges);
          if (context_ != null) {
            authzOk(context_, stmt_, withPrincipal);
          } else {
            authzOk(stmt_, withPrincipal);
          }
          output.validate(table);
        } finally {
          withPrincipal.cleanUp();
        }
      }
      return this;
    }

    /**
     * This method runs with the specified privileges.
     */
    public AuthzTest error(String expectedError, TPrivilege[]... privileges)
        throws ImpalaException {
      for (WithPrincipal withPrincipal: buildWithPrincipals()) {
        try {
          withPrincipal.init(privileges);
          if (context_ != null) {
            authzError(context_, stmt_, expectedError, withPrincipal);
          } else {
            authzError(stmt_, expectedError, withPrincipal);
          }
        } finally {
          withPrincipal.cleanUp();
        }
      }
      return this;
    }
  }

  protected AuthzTest authorize(String stmt) {
    return new AuthzTest(stmt);
  }

  protected AuthzTest authorize(AnalysisContext ctx, String stmt) {
    return new AuthzTest(ctx, stmt);
  }

  protected TPrivilege[] onServer(TPrivilegeLevel... levels) {
    return onServer(false, levels);
  }

  protected TPrivilege[] onServer(boolean grantOption, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.SERVER, false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  protected TPrivilege[] onDatabase(String db, TPrivilegeLevel... levels) {
    return onDatabase(false, db, levels);
  }

  protected TPrivilege[] onDatabase(boolean grantOption, String db,
      TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.DATABASE, false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setDb_name(db);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  protected TPrivilege[] onTable(String db, String table, TPrivilegeLevel... levels) {
    return onTable(false, db, table, levels);
  }

  protected TPrivilege[] onTable(boolean grantOption, String db, String table,
      TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.TABLE, false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setDb_name(db);
      privileges[i].setTable_name(table);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  protected TPrivilege[] onColumn(String db, String table, String column,
      TPrivilegeLevel... levels) {
    return onColumn(db, table, new String[]{column}, levels);
  }

  protected TPrivilege[] onColumn(boolean grantOption, String db, String table,
      String column, TPrivilegeLevel... levels) {
    return onColumn(grantOption, db, table, new String[]{column}, levels);
  }

  protected TPrivilege[] onColumn(String db, String table, String[] columns,
      TPrivilegeLevel... levels) {
    return onColumn(false, db, table, columns, levels);
  }

  protected TPrivilege[] onColumn(boolean grantOption, String db, String table,
      String[] columns, TPrivilegeLevel... levels) {
    int size = columns.length * levels.length;
    TPrivilege[] privileges = new TPrivilege[size];
    int idx = 0;
    for (int i = 0; i < levels.length; i++) {
      for (String column: columns) {
        privileges[idx] = new TPrivilege(levels[i], TPrivilegeScope.COLUMN, false);
        privileges[idx].setServer_name(SERVER_NAME);
        privileges[idx].setDb_name(db);
        privileges[idx].setTable_name(table);
        privileges[idx].setColumn_name(column);
        privileges[idx].setHas_grant_opt(grantOption);
        idx++;
      }
    }
    return privileges;
  }

  protected TPrivilege[] onUri(String uri, TPrivilegeLevel... levels) {
    return onUri(false, uri, levels);
  }

  protected TPrivilege[] onUri(boolean grantOption, String uri,
      TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.URI, false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setUri(uri);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  protected TPrivilege[] onStorageHandlerUri(String storageType, String storageUri,
      TPrivilegeLevel... levels) {
    return onStorageHandlerUri(false, storageType, storageUri, levels);
  }

  protected TPrivilege[] onStorageHandlerUri(boolean grantOption, String storageType,
      String storageUri, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.STORAGEHANDLER_URI,
          false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setStorage_type(storageType);
      privileges[i].setStorage_url(storageUri);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  protected TPrivilege[] onUdf(String db, String fn, TPrivilegeLevel... levels) {
    return onUdf(false, db, fn, levels);
  }

  protected TPrivilege[] onUdf(boolean grantOption, String db, String fn,
      TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege(levels[i], TPrivilegeScope.USER_DEFINED_FN, false);
      privileges[i].setServer_name(SERVER_NAME);
      privileges[i].setDb_name(db);
      privileges[i].setFn_name(fn);
      privileges[i].setHas_grant_opt(grantOption);
    }
    return privileges;
  }

  private void authzOk(String stmt, WithPrincipal withPrincipal) throws ImpalaException {
    authzOk(authzCtx_, stmt, withPrincipal, /* expectAnalysisOk */ true);
  }

  private void authzOk(String stmt, WithPrincipal withPrincipal,
      boolean expectAnalysisOk) throws ImpalaException {
    authzOk(authzCtx_, stmt, withPrincipal, expectAnalysisOk);
  }

  private void authzOk(AnalysisContext context, String stmt, WithPrincipal withPrincipal)
      throws ImpalaException {
    authzOk(context, stmt, withPrincipal, /* expectAnalysisOk */ true);
  }

  private void authzOk(AnalysisContext context, String stmt, WithPrincipal withPrincipal,
      boolean expectAnalysisOk) throws ImpalaException {
    try {
      LOG.info("Testing authzOk for {}", stmt);
      parseAndAnalyze(stmt, context, authzFrontend_);
    } catch (AuthorizationException e) {
      // Because the same test can be called from multiple statements
      // it is useful to know which statement caused the exception.
      throw new AuthorizationException(String.format(
          "\nPrincipal: %s\nStatement: %s\nError: %s", withPrincipal.getName(),
          stmt, e.getMessage(), e));
    } catch (AnalysisException e) {
      // We throw an AnalysisException only if we did not expect query analysis to fail.
      if (expectAnalysisOk) {
        throw new AnalysisException(String.format(
            "\nPrincipal: %s\nStatement: %s\nError: %s", withPrincipal.getName(),
            stmt, e.getMessage(), e));
      }
    }
  }

  /**
   * Verifies that a given statement fails authorization and the expected error
   * string matches.
   */
  private void authzError(String stmt, String expectedError,
      WithPrincipal withPrincipal) throws ImpalaException {
    authzError(authzCtx_, stmt, expectedError, startsWith(), withPrincipal);
  }

  private void authzError(AnalysisContext context, String stmt, String expectedError,
      WithPrincipal withPrincipal) throws ImpalaException {
    authzError(context, stmt, expectedError, startsWith(), withPrincipal);
  }

  @FunctionalInterface
  private interface Matcher {
    boolean match(String actual, String expected);
  }

  private static Matcher exact() {
    return (actual, expected) -> actual.equals(expected);
  }

  private static Matcher startsWith() {
    return (actual, expected) -> actual.startsWith(expected);
  }

  private void authzError(AnalysisContext ctx, String stmt,
      String expectedErrorString, Matcher matcher, WithPrincipal withPrincipal)
      throws ImpalaException {
    Preconditions.checkNotNull(expectedErrorString);
    try {
      LOG.info("Testing authzError for {}", stmt);
      parseAndAnalyze(stmt, ctx, authzFrontend_);
    } catch (AuthorizationException e) {
      // Insert the username into the error.
      expectedErrorString = String.format(expectedErrorString, ctx.getUser());
      String errorString = e.getMessage();
      assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          matcher.match(errorString, expectedErrorString));
      return;
    }
    fail(String.format("Statement did not result in authorization error.\n" +
        "Principal: %s\nStatement: %s", withPrincipal.getName(), stmt));
  }

  protected long createRangerPolicy(String policyName, String json) {
    ClientResponse response = rangerRestClient_
        .getResource("/service/public/v2/api/policy")
        .accept(RangerRESTUtils.REST_MIME_TYPE_JSON)
        .type(RangerRESTUtils.REST_MIME_TYPE_JSON)
        .post(ClientResponse.class, json);
    if (response.getStatusInfo().getFamily() != Family.SUCCESSFUL) {
      throw new RuntimeException(String.format(
          "Unable to create a Ranger policy: %s Response: %s",
          policyName, response.getEntity(String.class)));
    }
    String content = response.getEntity(String.class);
    JSONParser parser = new JSONParser();
    long policyId = -1;
    try {
      Object obj = parser.parse(content);
      policyId = (Long) ((JSONObject) obj).get("id");
    } catch (ParseException e) {
      LOG.error("Error parsing response content: {}", content);
    }
    LOG.info("Created ranger policy id={}, {}: {}", policyId, policyName, json);
    return policyId;
  }

  protected void deleteRangerPolicy(String policyName) {
    ClientResponse response = rangerRestClient_
        .getResource("/service/public/v2/api/policy")
        .queryParam("servicename", RANGER_SERVICE_NAME)
        .queryParam("policyname", policyName)
        .delete(ClientResponse.class);
    if (response.getStatusInfo().getFamily() != Family.SUCCESSFUL) {
      throw new RuntimeException(
          String.format("Unable to delete Ranger policy: %s.", policyName));
    }
    LOG.info("Deleted ranger policy {}", policyName);
  }

  // Convert TDescribeResult to list of strings.
  private static List<String> resultToStringList(TDescribeResult result) {
    List<String> list = new ArrayList<>();
    for (TResultRow row: result.getResults()) {
      for (TColumnValue col: row.getColVals()) {
        list.add(col.getString_val() == null ? "NULL": col.getString_val().trim());
      }
    }
    return list;
  }

  protected static String selectError(String object) {
    return "User '%s' does not have privileges to execute 'SELECT' on: " + object;
  }

  protected static String insertError(String object) {
    return "User '%s' does not have privileges to execute 'INSERT' on: " + object;
  }

  protected static String createError(String object) {
    return "User '%s' does not have privileges to execute 'CREATE' on: " + object;
  }

  protected static String rwstorageError(String object) {
    return "User '%s' does not have privileges to execute 'RWSTORAGE' on: " + object;
  }

  protected static String alterError(String object) {
    return "User '%s' does not have privileges to execute 'ALTER' on: " + object;
  }

  protected static String dropError(String object) {
    return "User '%s' does not have privileges to execute 'DROP' on: " + object;
  }

  protected static String accessError(String object) {
    return accessError(false, object);
  }

  protected static String accessError(boolean grantOption, String object) {
    return "User '%s' does not have privileges" +
        (grantOption ? " with 'GRANT OPTION'" : "") + " to access: " + object;
  }

  protected static String refreshError(String object) {
    return "User '%s' does not have privileges to execute " +
        "'INVALIDATE METADATA/REFRESH' on: " + object;
  }

  protected static String systemDbError() {
    return "Cannot modify system database.";
  }

  protected static String viewDefError(String object) {
    return "User '%s' does not have privileges to see the definition of view '" +
        object + "'.";
  }

  protected static String createFunctionError(String object) {
    return "User '%s' does not have privileges to CREATE functions in: " + object;
  }

  protected static String dropFunctionError(String object) {
    return "User '%s' does not have privileges to DROP functions in: " + object;
  }

  protected static String accessFunctionError(String object) {
    return "User '%s' does not have privileges to ANY functions in: " + object;
  }

  protected static String selectFunctionError(String object) {
    return "User '%s' does not have privileges to SELECT functions in: " + object;
  }

  protected static String columnMaskError(String object) {
    return "Column masking is disabled by --enable_column_masking flag. Can't access " +
        "column " + object + " that has column masking policy.";
  }

  protected static String rowFilterError(String object) {
    return "Row filtering is disabled by --enable_row_filtering flag. Can't access " +
        "table " + object + " that has row filtering policy.";
  }

  protected static String mvSelectError(String object) {
    return "Materialized view " +  object +
        " references tables with column masking or row filtering policies.";
  }

  protected ScalarFunction addFunction(String db, String fnName, List<Type> argTypes,
      Type retType, String uriPath, String symbolName) {
    ScalarFunction fn = ScalarFunction.createForTesting(db, fnName, argTypes, retType,
        uriPath, symbolName, null, null, TFunctionBinaryType.NATIVE);
    authzCatalog_.addFunction(fn);
    return fn;
  }

  protected ScalarFunction addFunction(String db, String fnName) {
    return addFunction(db, fnName, new ArrayList<>(), Type.INT, "/dummy",
        "dummy.class");
  }

  protected void removeFunction(ScalarFunction fn) {
    authzCatalog_.removeFunction(fn);
  }

  protected TPrivilegeLevel[] join(TPrivilegeLevel[] level1, TPrivilegeLevel... level2) {
    TPrivilegeLevel[] levels = new TPrivilegeLevel[level1.length + level2.length];
    int index = 0;
    for (TPrivilegeLevel level: level1) {
      levels[index++] = level;
    }
    for (TPrivilegeLevel level: level2) {
      levels[index++] = level;
    }
    return levels;
  }

  protected TPrivilegeLevel[] viewMetadataPrivileges() {
    return new TPrivilegeLevel[]{TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
        TPrivilegeLevel.SELECT, TPrivilegeLevel.INSERT, TPrivilegeLevel.REFRESH};
  }

  protected static TPrivilegeLevel[] allExcept(TPrivilegeLevel... excludedPrivLevels) {
    Set<TPrivilegeLevel> excludedSet = Sets.newHashSet(excludedPrivLevels);
    List<TPrivilegeLevel> privLevels = new ArrayList<>();
    for (TPrivilegeLevel level: TPrivilegeLevel.values()) {
      if (!excludedSet.contains(level)) {
        privLevels.add(level);
      }
    }
    return privLevels.toArray(new TPrivilegeLevel[0]);
  }
}

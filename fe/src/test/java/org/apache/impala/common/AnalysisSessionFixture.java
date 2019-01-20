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

package org.apache.impala.common;

import org.apache.impala.analysis.ExprRewriterTest;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;

/**
 * Session fixture for analyzer tests. Holds state shared across test cases such
 * as the front-end, the user, the database, and query options. Queries created
 * from this fixture start with these defaults, but each query can change them
 * as needed for that particular test case.
 *
 * This fixture is analogous to a user session. Though, unlike a real session,
 * test can change the database, options and user per-query without changing
 * the session settings.
 *
 * The session fixture is created once per test file, then query fixtures perform
 * the work needed for each particular query. It is often helpful to wrap the
 * query fixtures in a function if the same setup is used over and over.
 * See {@link ExprRewriterTest} for  example usage.
 */
public class AnalysisSessionFixture {

  private final FrontendFixture feFixture_ = FrontendFixture.instance();
  // Query options to be used for all queries. Can be overridden per-query.
  private final TQueryOptions queryOptions_;
  // Default database for all queries.
  private String db_ = Catalog.DEFAULT_DB;
  // Default user for all queries.
  private String user_ = System.getProperty("user.name");

  public AnalysisSessionFixture() {
    queryOptions_ = new TQueryOptions();
  }

  public AnalysisSessionFixture setDB(String db) {
    db_ = db;
    return this;
  }

  public AnalysisSessionFixture setUser(String user) {
    user_ = user;
    return this;
  }

  public TQueryOptions options() { return queryOptions_; }
  public String db() { return db_; }
  public String user() { return user_; }
  public Frontend frontend() { return feFixture_.frontend(); }
  public ImpaladTestCatalog catalog() { return feFixture_.catalog(); }

  /**
   * Disable the optional expression rewrites.
   */
  public AnalysisSessionFixture disableExprRewrite() {
    queryOptions_.setEnable_expr_rewrites(false);
    return this;
  }

  public TQueryOptions cloneOptions() {
    return new TQueryOptions(queryOptions_);
  }

  public TQueryCtx queryContext() {
    return TestUtils.createQueryContext(db_, user_, cloneOptions());
  }

}

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

package org.apache.impala.testutil;

import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.hive.executor.TestHiveJavaFunctionFactory;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TCopyTestCaseReq;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;

/**
 * A util class that loads a given testcase file into an in-memory Catalog
 * and runs EXPLAIN on the testcase query statement. The catalog is backed by a
 * transient HMS instanced based on derby DB.
 *
 * Runs in a standalone mode without having to start the Impala cluster.
 * Expects the testcase path as the only argument.
 * Ex: ...PlannerTestCaseLoader hdfs:///tmp/impala-testcase-data-9642a6f6...
 *
 * Full command example using maven-exec plugin (for an Impala DEV environment):
 *
 * mvn install exec:java -Dexec.classpathScope="test" \
 *    -Dexec.mainClass="org.apache.impala.testutil.PlannerTestCaseLoader" \
 *    -Dexec.args="/tmp/impala-testcase-data-9642a6f6-6a0b-48b2-a61f-f7ce55b92fee"
 *
 * Running through Maven is just for convenience. Can also be invoked directly using the
 * Java binary by setting appropriate classpath.
 *
 */
public class PlannerTestCaseLoader implements AutoCloseable {

  private final CatalogOpExecutor catalogOpExecutor_;
  private final ImpaladTestCatalog catalog_;
  private final Frontend frontend_;

  public PlannerTestCaseLoader() throws ImpalaException {
    catalog_ = new ImpaladTestCatalog(
        CatalogServiceTestCatalog.createTransientTestCatalog());
    frontend_ = new Frontend(new NoopAuthorizationFactory(), catalog_);
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_.getSrcCatalog(),
        new NoopAuthorizationFactory().getAuthorizationConfig(),
        new NoopAuthorizationManager(),
        new TestHiveJavaFunctionFactory());
  }

  public Catalog getSrcCatalog() { return catalog_.getSrcCatalog(); }

  /**
   * Loads the testcase from a given path and returns the EXPLAIN string for the
   * testcase query statement.
   */
  public String loadTestCase(String testCasePath) throws Exception {
    String stmt = catalogOpExecutor_.copyTestCaseData(new TCopyTestCaseReq(testCasePath),
        new TDdlExecResponse(new TCatalogUpdateResult()), /*wantMinimalResult*/false);
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        new TQueryOptions().setPlanner_testcase_mode(true));
    queryCtx.client_request.setStmt(stmt);
    return frontend_.getExplainString(queryCtx);
  }

  @Override
  public void close() {
    getSrcCatalog().close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException(String.format("Incorrect number of args. " +
          "Expected 1 argument, found %d. Valid usage: PlannerTestCaseLoader " +
          "<testcase path>", args.length));
    }
    try (PlannerTestCaseLoader testCaseLoader = new PlannerTestCaseLoader()) {
      System.out.println(testCaseLoader.loadTestCase(args[0]));
    }
    System.exit(0);
  }
}

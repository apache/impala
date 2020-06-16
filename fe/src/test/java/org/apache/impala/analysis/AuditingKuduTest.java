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

package org.apache.impala.analysis;

import com.google.common.collect.Sets;

import java.util.Set;

import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that auditing access events are properly captured during analysis for all
 * statement types on Kudu tables.
 */
public class AuditingKuduTest extends FrontendTestBase {
  @Test
  public void TestKuduStatements() throws AuthorizationException, AnalysisException {
    // Select
    Set<TAccessEvent> accessEvents =
        AnalyzeAccessEvents("select * from functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "SELECT")));

    // Insert
    accessEvents = AnalyzeAccessEvents(
        "insert into functional_kudu.testtbl (id) select id from " +
        "functional_kudu.alltypes");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.alltypes",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "INSERT")));

    // Upsert
    accessEvents = AnalyzeAccessEvents(
        "upsert into functional_kudu.testtbl (id) select id from " +
        "functional_kudu.alltypes");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.alltypes",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "ALL")));

    // Delete
    accessEvents = AnalyzeAccessEvents(
        "delete from functional_kudu.testtbl where id = 1");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "ALL")));

    // Delete using a complex query
    accessEvents = AnalyzeAccessEvents(
        "delete c from functional_kudu.testtbl c, functional_kudu.alltypes s where " +
        "c.id = s.id and s.int_col < 10");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.alltypes",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "ALL")));

    // Update
    accessEvents = AnalyzeAccessEvents(
        "update functional_kudu.testtbl set name = 'test' where id < 10");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "SELECT"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "ALL")));

    // Drop table
    accessEvents = AnalyzeAccessEvents("drop table functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(new TAccessEvent(
        "functional_kudu.testtbl", TCatalogObjectType.TABLE, "DROP")));

    // Drop table if exist
    accessEvents = AnalyzeAccessEvents("drop table if exists functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(new TAccessEvent(
            "functional_kudu.testtbl", TCatalogObjectType.TABLE, "DROP")));

    // Show create table
    accessEvents = AnalyzeAccessEvents("show create table functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(new TAccessEvent(
        "functional_kudu.testtbl", TCatalogObjectType.TABLE, "VIEW_METADATA")));

    // Compute stats
    accessEvents = AnalyzeAccessEvents("compute stats functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "ALTER"),
        new TAccessEvent("functional_kudu.testtbl",
                         TCatalogObjectType.TABLE, "SELECT")));

    // Describe
    accessEvents = AnalyzeAccessEvents("describe functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(new TAccessEvent(
        "functional_kudu.testtbl", TCatalogObjectType.TABLE, "ANY")));

    // Describe formatted
    accessEvents = AnalyzeAccessEvents("describe formatted functional_kudu.testtbl");
    Assert.assertEquals(accessEvents, Sets.newHashSet(new TAccessEvent(
        "functional_kudu.testtbl", TCatalogObjectType.TABLE, "ANY")));
  }
}

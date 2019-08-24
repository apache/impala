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

package org.apache.impala.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Catalog;
import org.junit.Test;

public class CatalogBlacklistUtilsTest {

  @Test
  public void testParsingBlacklistedDbs() {
    Set<String> blacklistedDbs;

    blacklistedDbs = CatalogBlacklistUtils.parseBlacklistedDbs("db1,db2", null);
    assertEquals(blacklistedDbs.size(), 2);
    assertTrue(blacklistedDbs.contains("db1"));
    assertTrue(blacklistedDbs.contains("db2"));

    // Test spaces
    blacklistedDbs = CatalogBlacklistUtils.parseBlacklistedDbs(" db1 , db2 ", null);
    assertEquals(blacklistedDbs.size(), 2);
    assertTrue(blacklistedDbs.contains("db1"));
    assertTrue(blacklistedDbs.contains("db2"));
    blacklistedDbs = CatalogBlacklistUtils.parseBlacklistedDbs(" ", null);
    assertTrue(blacklistedDbs.isEmpty());

    // Test lower/upper cases
    blacklistedDbs = CatalogBlacklistUtils.parseBlacklistedDbs("DB1,Db2", null);
    assertEquals(blacklistedDbs.size(), 2);
    assertTrue(blacklistedDbs.contains("db1"));
    assertTrue(blacklistedDbs.contains("db2"));

    // Test abnormal inputs
    blacklistedDbs = CatalogBlacklistUtils.parseBlacklistedDbs("db1,", null);
    assertEquals(blacklistedDbs.size(), 1);
    assertTrue(blacklistedDbs.contains("db1"));
  }

  @Test
  public void testParsingBlacklistedTables() {
    Set<TableName> blacklistedTables;

    blacklistedTables = CatalogBlacklistUtils.parseBlacklistedTables(
        "db3.foo,db3.bar", null);
    assertEquals(blacklistedTables.size(), 2);
    assertTrue(blacklistedTables.contains(new TableName("db3", "foo")));
    assertTrue(blacklistedTables.contains(new TableName("db3", "bar")));

    // Test spaces
    blacklistedTables = CatalogBlacklistUtils.parseBlacklistedTables(
        " db3 . foo , db3 . bar  ", null);
    assertEquals(blacklistedTables.size(), 2);
    assertTrue(blacklistedTables.contains(new TableName("db3", "foo")));
    assertTrue(blacklistedTables.contains(new TableName("db3", "bar")));

    // Test defaults
    blacklistedTables = CatalogBlacklistUtils.parseBlacklistedTables("foo", null);
    assertEquals(blacklistedTables.size(), 1);
    assertTrue(blacklistedTables.contains(new TableName(Catalog.DEFAULT_DB, "foo")));

    // Test lower/upper cases
    blacklistedTables = CatalogBlacklistUtils.parseBlacklistedTables(
        "DB3.Foo,db3.Bar", null);
    assertEquals(blacklistedTables.size(), 2);
    assertTrue(blacklistedTables.contains(new TableName("db3", "foo")));
    assertTrue(blacklistedTables.contains(new TableName("db3", "bar")));

    // Test abnormal inputs
    blacklistedTables = CatalogBlacklistUtils.parseBlacklistedTables("db3.,.bar,,", null);
    assertEquals(blacklistedTables.size(), 1);
    assertTrue(blacklistedTables.contains(new TableName(Catalog.DEFAULT_DB, "bar")));
  }
}

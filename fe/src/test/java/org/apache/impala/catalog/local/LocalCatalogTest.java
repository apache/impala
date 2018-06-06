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

package org.apache.impala.catalog.local;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.util.PatternMatcher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class LocalCatalogTest {

  private LocalCatalog catalog_;

  @Before
  public void setupCatalog() {
    catalog_ = new LocalCatalog(new DirectMetaProvider());
  }

  @Test
  public void testDbs() throws Exception {
    FeDb functionalDb = catalog_.getDb("functional");
    assertNotNull(functionalDb);
    FeDb functionalSeqDb = catalog_.getDb("functional_seq");
    assertNotNull(functionalSeqDb);

    Set<FeDb> dbs = ImmutableSet.copyOf(
        catalog_.getDbs(PatternMatcher.MATCHER_MATCH_ALL));
    assertTrue(dbs.contains(functionalDb));
    assertTrue(dbs.contains(functionalSeqDb));

    dbs = ImmutableSet.copyOf(
        catalog_.getDbs(PatternMatcher.createHivePatternMatcher("*_seq")));
    assertFalse(dbs.contains(functionalDb));
    assertTrue(dbs.contains(functionalSeqDb));
  }

  @Test
  public void testTables() throws Exception {
    Set<String> names = ImmutableSet.copyOf(catalog_.getTableNames(
        "functional", PatternMatcher.MATCHER_MATCH_ALL));
    assertTrue(names.contains("alltypes"));

    FeDb db = catalog_.getDb("functional");
    assertNotNull(db);
    FeTable t = catalog_.getTable("functional", "alltypes");
    assertNotNull(t);
    assertSame(t, db.getTable("alltypes"));
    assertSame(db, t.getDb());
    assertEquals("alltypes", t.getName());
    assertEquals("functional", t.getDb().getName());
    assertEquals("functional.alltypes", t.getFullName());
  }

}

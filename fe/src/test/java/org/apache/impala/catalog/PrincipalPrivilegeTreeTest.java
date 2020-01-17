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

package org.apache.impala.catalog;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;

public class PrincipalPrivilegeTreeTest {
  @Test
  public void testPrincipalPrivilegeTree() {
    // This test is mainly based on TestTreePrivilegeCache.testListPrivilegesWildCard in
    // Sentry.
    List<PrincipalPrivilege> privs = new ArrayList<>();

    privs.add(createTablePriv("server1", "db1", "t1"));
    privs.add(createTablePriv("server1", "db2", "t1"));
    privs.add(createTablePriv("server1", "db1", "t2"));
    privs.add(createDbPriv("server1", "db1"));
    privs.add(createServerPriv("server1"));
    privs.add(createServerPriv("server2"));
    privs.add(createColumnPriv("server1", "db1", "t1", "c1"));
    privs.add(createUriPriv("server1", "uri1"));

    PrincipalPrivilegeTree tree = new PrincipalPrivilegeTree();
    // Run the same tests twice to check if removing privileges works correctly.
    for (int i = 0; i < 2; i++) {
      for (PrincipalPrivilege priv : privs) tree.add(priv);

      // Update a privilege and check that the newer object is returned by
      // getFilteredList.
      PrincipalPrivilege newServer2Priv = createServerPriv("server2");
      tree.add(newServer2Priv);
      List<PrincipalPrivilege> results =
          tree.getFilteredList(createFilter("server2", null, null));
      assertEquals(1, results.size());
      assertSame(results.get(0), newServer2Priv);

      assertEquals(7, tree.getFilteredList(createFilter("server1", null, null)).size());
      assertEquals(5, tree.getFilteredList(createFilter("server1", "db1", null)).size());
      assertEquals(2, tree.getFilteredList(createFilter("server1", "db2", null)).size());
      assertEquals(4, tree.getFilteredList(createFilter("server1", "db1", "t1")).size());
      assertEquals(2, tree.getFilteredList(createFilter("server1", "db2", "t1")).size());
      assertEquals(2, tree.getFilteredList(createUriFilter("server1")).size());

      // Check that all privileges are removed successfully.
      for (PrincipalPrivilege priv : privs) tree.remove(priv);
      assertEquals(0, tree.getFilteredList(createFilter("server1", null, null)).size());
    }
  }

  PrincipalPrivilege createColumnPriv(String server, String db, String table,
      String column) {
    TPrivilege priv =
        new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.COLUMN, false);
    priv.setServer_name(server);
    priv.setDb_name(db);
    priv.setTable_name(table);
    priv.setColumn_name(column);
    return PrincipalPrivilege.fromThrift(priv);
  }

  PrincipalPrivilege createTablePriv(String server, String db, String table) {
    TPrivilege priv =
        new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.TABLE, false);
    priv.setServer_name(server);
    priv.setDb_name(db);
    priv.setTable_name(table);
    return PrincipalPrivilege.fromThrift(priv);
  }

  PrincipalPrivilege createDbPriv(String server, String db) {
    TPrivilege priv =
        new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.DATABASE, false);
    priv.setServer_name(server);
    priv.setDb_name(db);
    return PrincipalPrivilege.fromThrift(priv);
  }

  PrincipalPrivilege createUriPriv(String server, String uri) {
    TPrivilege priv =
        new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.URI, false);
    priv.setServer_name(server);
    priv.setUri(uri);
    return PrincipalPrivilege.fromThrift(priv);
  }

  PrincipalPrivilege createServerPriv(String server) {
    TPrivilege priv =
        new TPrivilege(TPrivilegeLevel.SELECT, TPrivilegeScope.SERVER, false);
    priv.setServer_name(server);
    return PrincipalPrivilege.fromThrift(priv);
  }

  PrincipalPrivilegeTree.Filter createFilter(String server, String db,
      String table) {
    PrincipalPrivilegeTree.Filter filter = new PrincipalPrivilegeTree.Filter();
    filter.setServer(server);
    filter.setDb(db);
    filter.setTable(table);
    filter.setIsUri(false);
    return filter;
  }

  PrincipalPrivilegeTree.Filter createUriFilter(String server) {
    PrincipalPrivilegeTree.Filter filter = new PrincipalPrivilegeTree.Filter();
    filter.setServer(server);
    filter.setIsUri(true);
    return filter;
  }
}

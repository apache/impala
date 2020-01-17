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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeScope;

import com.google.common.base.Preconditions;

/**
 * Tree that allows efficient lookup for all privileges that can be relevant in the
 * authorization of an object, e.g. getting privileges for server1/db1 should only iterate
 * through privileges in db1, but not in other databases.
 *
 * Performance characteristics:
 * add/remove: O(1), as the depth of the tree is limited
 * getFilteredList: O(number_of_privileges_that_match_the_filter)
 *
 * The primary motivation is to speed up SHOW DATABASES/TABLES calls which do a separate
 * listing to check access rights for each database/table. For this reason column and URI
 * privileges do not get their own "level" in the tree - column privileges are merged with
 * table privileges and URI privileges are stored per-server. This avoids unnecessarily
 * storing many small hash maps in memory.
 *
 * This class is expected to be used in pair with a CatalogObjectCache<PrincipalPrivilege>
 * which also handles catalog version logic and has efficient "remove" by full name.
 */
public class PrincipalPrivilegeTree {
   // Contains database/table/column privileges grouped by server name + database name
   // + table name. Column privileges are stored with table privileges to avoid the
   // memory cost of creating many small hashmaps for columns with privileges.
   Node<PrincipalPrivilege> tableRoot_ = new Node<>();

   // Contains URI privileges grouped by server name. Storing these separately from table
   // privileges allows Node to be simpler, as the Server level doesn't need to
   // differentiate between two kind of privileges.
   Node<PrincipalPrivilege> uriRoot_ = new Node<>();

   // Contains server privileges grouped by server name. Stored separately from other
   // privileges, as these should be returned both when listing URI and non-URI
   // privileges.
   Node<PrincipalPrivilege> serverRoot_ = new Node<>();

   public void add(PrincipalPrivilege privilege) {
     TPrivilege priv = privilege.toThrift();
     List<String> path = toPath(priv);
     Node<PrincipalPrivilege> root = getRootForScope(priv.getScope());
     root.add(privilege.getName(), privilege, path);
   }

   public void remove(PrincipalPrivilege privilege) {
     TPrivilege priv = privilege.toThrift();
     List<String> path = toPath(priv);
     Node<PrincipalPrivilege> root = getRootForScope(priv.getScope());
     root.remove(privilege.getName(), privilege, path);
   }

   /**
   * Collect all privileges that match the filter.
   * E.g. for server1.db1, it returns privileges on:
   *  server1, server1.db1, server1.db1.table1,
   * but not privileges on:
   *  server2, server2.db1, server1.db2
   */
   public List<PrincipalPrivilege> getFilteredList(Filter filter) {
     List<String> path = filter.toPath();
     List<PrincipalPrivilege> results = new ArrayList<>();
     List<Node<PrincipalPrivilege>> roots = new ArrayList<>();
     // Server level privileges apply to both URIs and other objects.
     roots.add(serverRoot_);
     if (filter.isUri_ || path.size() == 1) roots.add(uriRoot_);
     if (!filter.isUri_) roots.add(tableRoot_);
     for (Node<PrincipalPrivilege> root : roots) {
       root.getAllMatchingValues(results, path);
     }
     return results;
   }

   private Node<PrincipalPrivilege> getRootForScope(TPrivilegeScope scope) {
     switch(scope) {
     case URI: return uriRoot_;
     case SERVER: return serverRoot_;
     default: return tableRoot_;
     }
   }

   /**
    * Creates a path to the given privilege in the tree like ["server1", "db1", "table1"].
    */
   private static List<String> toPath(TPrivilege priv) {
      List<String> path = new ArrayList<>();
      String server = priv.getServer_name();
      String db = priv.getDb_name();
      String table = priv.getTable_name();
      if (server == null) return path;
      path.add(server.toLowerCase());
      if (db == null) return path;
      path.add(db.toLowerCase());
      if (table != null) path.add(table.toLowerCase());
      return path;
   }

  /**
   * Lossy representation of an Authorizable (doesn't contain column/URI name).
   * Can be used to rule out the bulk of the privileges that can have no effect on an
   * access check.
   */
  public static class Filter {
    String server_, db_, table_; // must be lower case, null matches everything
    boolean isUri_ = false;

    public void setServer(String server) { server_ = server; }
    public void setDb(String db) { db_ = db; }
    public void setTable(String  table) { table_ = table; }
    public void setIsUri(boolean isUri) { isUri_ = isUri; }

   /**
    * Creates a path till the first null element of the filter, e.g.
    * ["server1", "db1"] if table_ is null.
    */
    private List<String> toPath() {
      List<String> path = new ArrayList<>();
      if (server_ == null) return path;
      path.add(server_);
      if (db_ == null) return path;
      Preconditions.checkState(!isUri_);
      path.add(db_);
      if (table_ != null) path.add(table_);
      return path;
    }
  }

  /**
   * Tree node that holds the privileges for a given object (server, database, table),
   * and its children objects (e.g. databases for a server). Descendants can be addressed
   * with paths like ["server1", "db1", "table1"].
   *
   * Only used to store privileges, but creating a generic class seemed clearer.
   */
  private static class Node<T> {
    Map<String, T> values_ = null;
    Map<String, Node<T>> children_= null;

    boolean isEmpty() { return values_ == null && children_ == null; }

    /**
     * Finds the Node at 'path' (or potentially builds it if it doesn't exist),
     * and adds 'key' + 'value' to it.
     */
    public void add(String key, T value, List<String> path) {
      add(key, value, path, 0);
    }

    /**
     * Finds the Node at 'path' (it is treated as error if it doesn't exist),
     * and removes 'key' + 'value' from it. If a Node becomes empty, it is removed from
     * it's parent.
     */
    public void remove(String key, T value, List<String> path) {
      remove(key, value, path, 0);
    }

    /**
     * Collect all values in this node and those descendants that match 'path'.
     */
    public void getAllMatchingValues(List<T> results, List<String> path) {
      getAllMatchingValues(results, path, 0);
    }

    /**
     * Collect all values in this node and its descendants.
     */
    public void getAllValues(List<T> results) {
      if (values_ != null) results.addAll(values_.values());
      if (children_ != null) {
        for (Map.Entry<String, Node<T>> entry : children_.entrySet()) {
          entry.getValue().getAllValues(results);
        }
      }
    }

    private void add(String key, T value, List<String> path, int depth) {
      if (path.size() <= depth) {
        if (values_ == null) {
          // It is very common to have only a single privilege on an object
          // (e.g. ownership), so the initial capacity is 1 to avoid wasting memory.
          values_ = new HashMap<>(1);
        }
        values_.put(key, value); // Can update an existing value.
      } else {
        if (children_ == null) children_ = new HashMap<>();
        String child_name = path.get(depth);
        Node<T> next = children_.computeIfAbsent(child_name, (k) -> new Node<T>());
        next.add(key, value, path, depth + 1);
      }
    }

    private void remove(String key, T value, List<String> path, int depth) {
      if (path.size() <= depth) {
        Preconditions.checkNotNull(values_);
        T found = values_.remove(key);
        Preconditions.checkNotNull(found);
        if (values_.isEmpty()) values_ = null;
      } else {
        Preconditions.checkNotNull(children_);
        String child_name = path.get(depth);
        Node<T> next = children_.get(child_name);
        Preconditions.checkNotNull(next);
        next.remove(key, value, path, depth + 1);
        // Remove the child if it became empty.
        if (next.isEmpty()) children_.remove(child_name);
        if (children_.isEmpty()) children_ = null;
      }
    }

    private void getAllMatchingValues(List<T> results, List<String> path, int depth) {
      if (path.size() <= depth) {
        getAllValues(results);
        return;
      }
      if (values_ != null) results.addAll(values_.values());
      if (children_ == null) return;
      String child_name = path.get(depth);
      Preconditions.checkNotNull(child_name);
      Node<T> next = children_.get(child_name);
      if (next != null) next.getAllMatchingValues(results, path, depth + 1);
    }
  }
}

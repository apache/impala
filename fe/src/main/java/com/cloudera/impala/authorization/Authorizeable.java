// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.authorization;

import java.util.List;

/*
 * Interface all authorizeable objects (Table, Db, Column, etc) must implement.
 */
public interface Authorizeable {
  /*
  * Returns the list of the Hive "authorizeable" objects in their hierarchical order.
  * For example:
  * [Column] would return Db -> Table -> Column
  * [Table] would return Db -> Table
  * [Db] would return [Db]
  * [URI] would return [URI]
  */
  public List<org.apache.access.core.Authorizable> getHiveAuthorizeableHierarchy();

  // Returns the name of the object.
  public String getName();
}
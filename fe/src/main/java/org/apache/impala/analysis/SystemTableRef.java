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

import org.apache.impala.catalog.FeSystemTable;

import com.google.common.base.Preconditions;

/**
 * TableRef class for system tables.
 *
 * Represents a table that is registered as a normal table in HMS, but content is
 * constructed in-memory. Currently COMPUTE STATS does not work on these tables, and
 * write operations are not allowed.
 */
public class SystemTableRef extends BaseTableRef {

  public SystemTableRef(TableRef tableRef, Path resolvedPath) {
    super(tableRef, resolvedPath);
    Preconditions.checkState(resolvedPath.getRootTable() instanceof FeSystemTable);
  }
}

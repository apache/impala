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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;

/**
 * Interface for loading metadata. See {@link LocalCatalog} for an example.
 *
 * Implementations may directly access the metadata from the source systems
 * or may include caching, etc.
 *
 * TODO(IMPALA-7127): expand this to include file metadata, sentry metadata,
 * etc.
 */
interface MetaProvider {
  ImmutableList<String> loadDbList() throws TException;

  Database loadDb(String dbName) throws TException;

  ImmutableList<String> loadTableNames(String dbName)
      throws MetaException, UnknownDBException, TException;

  Table loadTable(String dbName, String tableName)
      throws NoSuchObjectException, MetaException, TException;

  String loadNullPartitionKeyValue()
      throws MetaException, TException;

  List<String> loadPartitionNames(String dbName, String tableName)
      throws MetaException, TException;

  /**
   * Load the given partitions from the specified table.
   *
   * If a requested partition does not exist, no exception will be thrown.
   * Instead, the resulting map will contain no entry for that partition.
   */
  Map<String, Partition> loadPartitionsByNames(String dbName, String tableName,
      List<String> partitionColumnNames, List<String> partitionNames)
      throws MetaException, TException;
}

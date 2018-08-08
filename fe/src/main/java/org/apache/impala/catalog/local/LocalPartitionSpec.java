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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.util.MetaStoreUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

/**
 * Specification of a partition of a {@link LocalFsTable} containing
 * everything necessary for partition pruning.
 */
@Immutable
class LocalPartitionSpec implements PrunablePartition {
  private final long id_;
  private final String name_;

  // LiteralExprs are technically mutable prior to analysis.
  @SuppressWarnings("Immutable")
  private final ImmutableList<LiteralExpr> partitionValues_;

  LocalPartitionSpec(LocalFsTable table, String partName, long id) {
    id_ = id;
    name_ = Preconditions.checkNotNull(partName);
    if (!partName.isEmpty()) {
      try {
        List<String> partValues = MetaStoreUtil.getPartValsFromName(
            table.getMetaStoreTable(), partName);
        partitionValues_ = ImmutableList.copyOf(FeCatalogUtils.parsePartitionKeyValues(
            table, partValues));
      } catch (CatalogException | MetaException e) {
        throw new LocalCatalogException(String.format(
            "Failed to parse partition name '%s' for table %s",
            partName, table.getFullName()), e);
      }
    } else {
      // Unpartitioned tables have a single partition with empty name.
      partitionValues_= ImmutableList.of();
    }
  }

  @Override
  public long getId() { return id_; }

  @Override
  public List<LiteralExpr> getPartitionValues() { return partitionValues_; }

  String getName() { return name_; }
}

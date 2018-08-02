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

import javax.annotation.Nullable;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.thrift.CatalogObjectsConstants;
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
  static final long UNPARTITIONED_ID = 0;
  private final long id_;

  @Nullable
  private final PartitionRef ref_;

  // LiteralExprs are technically mutable prior to analysis.
  @SuppressWarnings("Immutable")
  private final ImmutableList<LiteralExpr> partitionValues_;

  LocalPartitionSpec(LocalFsTable table, PartitionRef ref, long id) {
    id_ = id;
    ref_ = Preconditions.checkNotNull(ref);
    if (ref.getName().isEmpty()) {
      // "unpartitioned" partition
      partitionValues_ = ImmutableList.of();
      return;
    }
    try {
      List<String> partValues = MetaStoreUtil.getPartValsFromName(
          table.getMetaStoreTable(), ref_.getName());
      partitionValues_ = ImmutableList.copyOf(FeCatalogUtils.parsePartitionKeyValues(
          table, partValues));
    } catch (CatalogException | MetaException e) {
      throw new LocalCatalogException(String.format(
          "Failed to parse partition name '%s' for table %s",
          ref.getName(), table.getFullName()), e);
    }
  }

  LocalPartitionSpec(LocalFsTable table, long id) {
    // Unpartitioned tables have a single partition with empty name.
    Preconditions.checkArgument(id == CatalogObjectsConstants.PROTOTYPE_PARTITION_ID ||
        id == UNPARTITIONED_ID);
    this.id_ = id;
    this.ref_ = null;
    partitionValues_= ImmutableList.of();
  }

  @Override
  public long getId() { return id_; }

  @Override
  public List<LiteralExpr> getPartitionValues() { return partitionValues_; }

  PartitionRef getRef() { return ref_; }

  @Override
  public String toString() {
    if (ref_ != null) {
      return ref_.getName();
    } else if (id_ == CatalogObjectsConstants.PROTOTYPE_PARTITION_ID) {
      return "<prototype>";
    } else {
      return "<default>";
    }

  }
}

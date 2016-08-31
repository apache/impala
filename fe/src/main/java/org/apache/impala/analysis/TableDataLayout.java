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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Represents the PARTITION BY and DISTRIBUTED BY clauses of a DDL statement.
 * TODO: Reconsider this class when we add support for new range partitioning syntax (see
 * IMPALA-3724).
 */
class TableDataLayout {

  private final List<ColumnDef> partitionColDefs_;
  private final List<DistributeParam> distributeParams_;

  private TableDataLayout(List<ColumnDef> partitionColumnDefs,
      List<DistributeParam> distributeParams) {
    partitionColDefs_ = partitionColumnDefs;
    distributeParams_ = distributeParams;
  }

  static TableDataLayout createPartitionedLayout(List<ColumnDef> partitionColumnDefs) {
    return new TableDataLayout(partitionColumnDefs,
        Lists.<DistributeParam>newArrayList());
  }

  static TableDataLayout createDistributedLayout(List<DistributeParam> distributeParams) {
    return new TableDataLayout(Lists.<ColumnDef>newArrayList(), distributeParams);
  }

  static TableDataLayout createEmptyLayout() {
    return new TableDataLayout(Lists.<ColumnDef>newArrayList(),
        Lists.<DistributeParam>newArrayList());
  }

  List<ColumnDef> getPartitionColumnDefs() { return partitionColDefs_; }
  List<DistributeParam> getDistributeParams() { return distributeParams_; }
}

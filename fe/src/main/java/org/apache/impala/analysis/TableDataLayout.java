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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the PARTITION BY and PARTITIONED BY clauses of a DDL statement.
 * We can use PARTITIONED BY SPEC clause to create iceberg table partitions.
 */
class TableDataLayout {

  private final List<ColumnDef> partitionColDefs_;
  private final List<KuduPartitionParam> kuduPartitionParams_;
  private final List<IcebergPartitionSpec> icebergPartitionSpecs_;

  private TableDataLayout(List<ColumnDef> partitionColumnDefs,
                          List<KuduPartitionParam> partitionParams,
                          List<IcebergPartitionSpec> icebergPartitionSpecs) {
    partitionColDefs_ = partitionColumnDefs;
    kuduPartitionParams_ = partitionParams;
    icebergPartitionSpecs_ = icebergPartitionSpecs;
  }

  static TableDataLayout createPartitionedLayout(List<ColumnDef> partitionColumnDefs) {
    return new TableDataLayout(partitionColumnDefs,
        new ArrayList<>(), new ArrayList<>());
  }

  static TableDataLayout createKuduPartitionedLayout(
      List<KuduPartitionParam> partitionParams) {
    return new TableDataLayout(new ArrayList<>(), partitionParams, new ArrayList<>());
  }

  static TableDataLayout createIcebergPartitionedLayout(
      List<IcebergPartitionSpec> icebergPartitionSpecs) {
    return new TableDataLayout(new ArrayList<>(), new ArrayList<>(),
        icebergPartitionSpecs);
  }

  static TableDataLayout createEmptyLayout() {
    return new TableDataLayout(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  List<ColumnDef> getPartitionColumnDefs() { return partitionColDefs_; }
  List<KuduPartitionParam> getKuduPartitionParams() { return kuduPartitionParams_; }
  List<IcebergPartitionSpec> getIcebergPartitionSpecs() { return icebergPartitionSpecs_; }
}

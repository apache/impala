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

import java.util.List;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.planner.HdfsPartitionPruner;

/**
 * A specification of a partition that is part of a table, sufficient
 * for pruning by {@link HdfsPartitionPruner}.
 *
 * This interface only exposes the partition ID and values, so it can
 * be implemented by only fetching partition names and not the complete
 * partition metadata.
 */
public interface PrunablePartition {
  /**
   * Returns the identifier of this partition, suitable for later passing
   * to {@link FeFsTable#loadPartitions(java.util.Collection)}
   */
  public long getId();

  /**
   * Returns the values associated with this partition
   */
  List<LiteralExpr> getPartitionValues();
}

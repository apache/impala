/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

import java.util.List;
import java.util.stream.Collectors;

/*
 * Extract the partition field related expressions from pushable predicates.
 */
public class PaimonPartitionPredicateVisitor implements PredicateVisitor<Boolean> {
  // Case in-sensitive partition key comparision
  private final List<String> partitionKeys;

  public PaimonPartitionPredicateVisitor(List<String> partitionKeys) {
    this.partitionKeys =
        partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
  }

  public Boolean visit(LeafPredicate predicate) {
    return this.partitionKeys.contains(predicate.fieldName().toLowerCase());
  }

  /*
   * This function will check whether the given predicate is partition-only predicate
   * expression, if yes, the partition predicate will not be added to the postscan
   * expressions to reduce filter cost. It assumed that all partition-only predicate
   * can accurately filter out partitions by the given partition-only predicate.
   * */
  public Boolean visit(CompoundPredicate predicate) {
    for (Predicate child : predicate.children()) {
      Boolean matched = (Boolean) child.visit(this);
      if (!matched) return false;
    }
    return true;
  }
}

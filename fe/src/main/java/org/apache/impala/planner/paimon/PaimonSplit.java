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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.Split;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Paimon split entity used by paimon jni scanner.
 * */
public class PaimonSplit implements Serializable {
  // Paimon split instance to perform scan.
  private final Split split_;
  // predicates that can be pushed to paimon source.
  private final ArrayList<Predicate> predicates_;

  public PaimonSplit(Split split, ArrayList<Predicate> predicates) {
    split_ = split;
    predicates_ = predicates;
  }

  public Split getSplit() { return split_; }

  public List<Predicate> getPredicates() { return predicates_; }
}

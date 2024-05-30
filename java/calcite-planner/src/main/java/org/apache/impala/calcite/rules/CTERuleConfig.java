/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rules;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.impala.calcite.rel.node.ImpalaCTEConsumer;
import org.immutables.value.Value;

@Value.Immutable
public interface CTERuleConfig extends RelRule.Config {

  Map<List<String>, Integer> tableOccurrences();

  int referenceThreshold();

  static CTERuleConfig create(
      int referenceThreshold, Map<List<String>, Integer> tableOccurences) {
    return ImmutableCTERuleConfig.builder().operandSupplier(
        b -> b.operand(ImpalaCTEConsumer.class).noInputs())
        .referenceThreshold(referenceThreshold).tableOccurrences(tableOccurences).build();
  }

  @Override
  default RelOptRule toRule() {
    throw new IllegalStateException("Must not be called");
  }
}

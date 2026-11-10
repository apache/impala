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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class PaimonJniReadFilterTest {
  private static final RowType ROW_TYPE = RowType.of(
      new DataType[] {
          DataTypes.INT(), DataTypes.FLOAT(), DataTypes.BIGINT(), DataTypes.INT()},
      new String[] {"movieid", "rating", "ts", "userid"});

  @Test
  public void testRemapPartitionColumnToPartitionRowIndex() {
    PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
    // userid is field 3 in the table row and field 0 in the partition row.
    Predicate predicate = builder.equal(3, 1);
    Optional<Predicate> remapped = PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.singletonList("UserId"), predicate);
    assertTrue(remapped.isPresent());
    assertEquals(0, ((LeafPredicate) remapped.get()).index());
  }

  @Test
  public void testNonPartitionFieldIsNotRemapped() {
    PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
    Predicate predicate = builder.equal(0, 3);
    Optional<Predicate> remapped = PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.singletonList("userid"), predicate);
    assertFalse(remapped.isPresent());
    // A predicate that does not remap stays a data filter and is re-checked by Impala.
    PaimonJniReadFilter filters = PaimonJniReadFilter.of(
        Collections.emptyList(), Collections.singletonList(predicate));
    assertTrue(filters.getPartitionPredicates().isEmpty());
    assertEquals(1, filters.getDataFilters().size());
    assertEquals(0, ((LeafPredicate) filters.getDataFilters().get(0)).index());
  }

  @Test
  public void testMappingUsesPartitionKeyOrder() {
    int[] mapping = PaimonJniReadFilter.partitionIndexMapping(
        ROW_TYPE, Arrays.asList("userid", "movieid"));
    assertEquals(4, mapping.length);
    assertEquals(1, mapping[0]);
    assertEquals(-1, mapping[1]);
    assertEquals(-1, mapping[2]);
    assertEquals(0, mapping[3]);
  }

  @Test
  public void testCompoundPredicateRemap() {
    PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
    // Both leaves are partition fields (userid): the compound remaps.
    Predicate partitionOnly = PredicateBuilder.and(
        builder.equal(3, 1), builder.lessThan(3, 5));
    Optional<Predicate> remapped = PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.singletonList("userid"), partitionOnly);
    assertTrue(remapped.isPresent());
    CompoundPredicate compound = (CompoundPredicate) remapped.get();
    assertEquals(2, compound.children().size());
    for (Predicate child : compound.children()) {
      assertEquals(0, ((LeafPredicate) child).index());
    }
    // OR mixing a partition field and a data field cannot be remapped: partition
    // pruning would then depend on a non-partition value.
    Predicate mixed = PredicateBuilder.or(builder.equal(3, 1), builder.equal(0, 3));
    remapped = PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.singletonList("userid"), mixed);
    assertFalse(remapped.isPresent());
  }

  @Test
  public void testNullAndEmptyArguments() {
    PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
    Predicate predicate = builder.equal(3, 1);
    assertFalse(PaimonJniReadFilter.tryRemapToPartitionRow(
        null, Collections.singletonList("userid"), predicate).isPresent());
    assertFalse(PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.emptyList(), predicate).isPresent());
    assertFalse(PaimonJniReadFilter.tryRemapToPartitionRow(
        ROW_TYPE, Collections.singletonList("userid"), null).isPresent());
  }
}

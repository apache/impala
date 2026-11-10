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

import com.google.common.collect.Lists;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Partition and data predicates for Paimon JNI scans. Serialized once per scan fragment
 * as {@code paimon_read_filters} on the plan and JNI scan param.
 *
 * The coordinator may call {@link #applyTo} for split planning; executors deserialize the
 * same payload in {@link org.apache.impala.util.paimon.PaimonJniScanner}.
 *
 * Partition predicates are indexed into the partition row (partition-key order), not the
 * full table row. {@link #applyTo} must not remap them again. Data filters keep
 * full-row field indexes; Paimon projects them when the scan applies projection.
 *
 * {@link #fromPushedPredicates} splits pushed predicates into partition and data filters,
 * mirroring paimon-spark {@code PaimonBaseScanBuilder.pushPredicates}.
 */
public final class PaimonJniReadFilter implements Serializable {
  private final ArrayList<Predicate> partitionPredicates_;
  private final ArrayList<Predicate> dataFilters_;

  private PaimonJniReadFilter(
      List<Predicate> partitionPredicates, List<Predicate> dataFilters) {
    partitionPredicates_ = new ArrayList<>(partitionPredicates);
    dataFilters_ = new ArrayList<>(dataFilters);
  }

  public List<Predicate> getPartitionPredicates() { return partitionPredicates_; }

  public List<Predicate> getDataFilters() { return dataFilters_; }

  public static PaimonJniReadFilter empty() {
    return new PaimonJniReadFilter(Lists.newArrayList(), Lists.newArrayList());
  }

  public static PaimonJniReadFilter of(
      List<Predicate> partitionPredicates, List<Predicate> dataFilters) {
    return new PaimonJniReadFilter(partitionPredicates, dataFilters);
  }

  /**
   * Remaps {@code predicate} onto the partition row. Returns empty when the predicate
   * references a non-partition field or a field that cannot be mapped. The mapping is
   * case-insensitive so it matches {@link PaimonPartitionPredicateVisitor}.
   */
  public static Optional<Predicate> tryRemapToPartitionRow(
      RowType predicateRowType, List<String> partitionKeys, Predicate predicate) {
    if (predicateRowType == null || partitionKeys == null || partitionKeys.isEmpty()
        || predicate == null) {
      return Optional.empty();
    }
    if (!predicate.visit(new PaimonPartitionPredicateVisitor(partitionKeys))) {
      return Optional.empty();
    }
    return PredicateBuilder.transformFieldMapping(
        predicate, partitionIndexMapping(predicateRowType, partitionKeys));
  }

  /**
   * For each field index in {@code predicateRowType}, the value is that field's index in
   * {@code partitionKeys}, or -1 when the field is not a partition key.
   */
  static int[] partitionIndexMapping(
      RowType predicateRowType, List<String> partitionKeys) {
    List<String> loweredKeys = Lists.newArrayListWithCapacity(partitionKeys.size());
    for (String key : partitionKeys) {
      loweredKeys.add(key.toLowerCase(Locale.ROOT));
    }
    int fieldCount = predicateRowType.getFieldCount();
    int[] mapping = new int[fieldCount];
    Arrays.fill(mapping, -1);
    List<DataField> fields = predicateRowType.getFields();
    for (int i = 0; i < fieldCount; ++i) {
      int partIdx = loweredKeys.indexOf(fields.get(i).name().toLowerCase(Locale.ROOT));
      if (partIdx >= 0) { mapping[i] = partIdx; }
    }
    return mapping;
  }

  public static PaimonJniReadFilter fromPushedPredicates(
      Table table, List<Predicate> pushedPredicates) {
    if (pushedPredicates == null || pushedPredicates.isEmpty()) {
      return empty();
    }
    RowType predicateRowType = ImpalaPaimonRowTypeUtils.predicateRowType(table);
    List<String> partitionKeys = table.partitionKeys();
    List<Predicate> partitionOnly = Lists.newArrayList();
    List<Predicate> dataOnly = Lists.newArrayList();
    for (Predicate predicate : pushedPredicates) {
      Optional<Predicate> remapped =
          tryRemapToPartitionRow(predicateRowType, partitionKeys, predicate);
      if (remapped.isPresent()) {
        partitionOnly.add(remapped.get());
      } else {
        // Keep the full-row predicate. Partition pruning is exact, so a predicate that
        // cannot be remapped must not be installed as a partition filter.
        dataOnly.add(predicate);
      }
    }
    return of(partitionOnly, dataOnly);
  }

  public static byte[] serialize(PaimonJniReadFilter readFilter) {
    if (readFilter == null
        || (readFilter.partitionPredicates_.isEmpty()
            && readFilter.dataFilters_.isEmpty())) {
      return null;
    }
    return SerializationUtils.serialize(readFilter);
  }

  public static PaimonJniReadFilter deserialize(byte[] data) {
    if (data == null || data.length == 0) {
      return empty();
    }
    return SerializationUtils.deserialize(data);
  }

  /**
   * Thrift binary fields are slices of a larger buffer. Copy only the remaining
   * bytes; {@link ByteBuffer#array()} can include bytes outside this value.
   *
   * @see org.apache.impala.util.paimon.PaimonJniScanner
   */
  public static PaimonJniReadFilter deserialize(ByteBuffer data) {
    if (data == null || !data.hasRemaining()) {
      return empty();
    }
    ByteBuffer slice = data.slice();
    byte[] bytes = new byte[slice.remaining()];
    slice.get(bytes);
    return deserialize(bytes);
  }

  public void applyTo(Table table, ReadBuilder readBuilder) {
    if (!partitionPredicates_.isEmpty()) {
      RowType partitionType = table.rowType().project(table.partitionKeys());
      Predicate merged = mergePredicates(partitionPredicates_);
      PartitionPredicate partitionFilter =
          PartitionPredicate.fromPredicate(partitionType, merged);
      if (partitionFilter != null) {
        readBuilder.withPartitionFilter(partitionFilter);
      }
    }
    if (!dataFilters_.isEmpty()) {
      readBuilder.withFilter(dataFilters_);
    }
  }

  private static Predicate mergePredicates(List<Predicate> predicates) {
    if (predicates.size() == 1) { return predicates.get(0); }
    return PredicateBuilder.and(predicates);
  }
}

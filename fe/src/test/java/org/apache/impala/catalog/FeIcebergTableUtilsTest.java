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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.IcebergPartitionTransform;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.junit.Test;

/**
 * Tests for FeIcebergTable.Utils.getDefaultPartitionSpec(). Partition spec ids are not
 * positions in FeIcebergTable.getPartitionSpecs(), e.g. Iceberg's
 * ExpireSnapshots.cleanExpiredMetadata(true) removes unused partition specs
 * (IMPALA-15461).
 */
public class FeIcebergTableUtilsTest {
  private static final String TABLE_NAME = "db.t";

  /**
   * Returns a partition spec with id 'specId' that has an identity partition field for
   * each of 'cols'.
   */
  private static IcebergPartitionSpec spec(int specId, String... cols) {
    List<IcebergPartitionField> fields = new ArrayList<>();
    for (int i = 0; i < cols.length; ++i) {
      fields.add(new IcebergPartitionField(i + 1, 1000 + i, cols[i], cols[i],
          new IcebergPartitionTransform(TIcebergPartitionTransformType.IDENTITY),
          Type.STRING));
    }
    return new IcebergPartitionSpec(specId, fields);
  }

  /**
   * Returns a mocked FeIcebergTable with partition specs 'specs' and default partition
   * spec id 'defaultSpecId'. Like IcebergTable, its Iceberg API table's current spec has
   * id 'defaultSpecId' too.
   */
  private static FeIcebergTable mockTable(List<IcebergPartitionSpec> specs,
      int defaultSpecId) {
    Table iceApiTable = mock(Table.class);
    when(iceApiTable.spec()).thenReturn(
        PartitionSpec.builderFor(new Schema()).withSpecId(defaultSpecId).build());
    FeIcebergTable table = mock(FeIcebergTable.class);
    when(table.getIcebergApiTable()).thenReturn(iceApiTable);
    when(table.getPartitionSpecs()).thenReturn(specs);
    when(table.getPartitionSpec(anyInt())).thenCallRealMethod();
    when(table.getDefaultPartitionSpecId()).thenReturn(defaultSpecId);
    when(table.getFullName()).thenReturn(TABLE_NAME);
    return table;
  }

  private static List<String> fieldNames(IcebergPartitionSpec spec) {
    return spec.getIcebergPartitionFields().stream()
        .map(IcebergPartitionField::getFieldName)
        .collect(Collectors.toList());
  }

  /**
   * Asserts that the default partition spec of a table with partition specs 'specs' and
   * default partition spec id 'defaultSpecId' is the spec with that id, which has
   * identity partition fields for 'expectedCols'.
   */
  private static void assertDefaultSpec(List<IcebergPartitionSpec> specs,
      int defaultSpecId, String... expectedCols) {
    IcebergPartitionSpec spec =
        Utils.getDefaultPartitionSpec(mockTable(specs, defaultSpecId));
    assertNotNull(spec);
    assertEquals(defaultSpecId, spec.getSpecId());
    assertEquals(ImmutableList.copyOf(expectedCols), fieldNames(spec));
  }

  @Test
  public void testDefaultPartitionSpecIsLookedUpById() {
    // The only spec has id 1, e.g. after cleanExpiredMetadata() removed spec 0.
    assertDefaultSpec(ImmutableList.of(spec(1, "region")), 1, "region");
    // Position 1 holds spec 2.
    assertDefaultSpec(ImmutableList.of(spec(1), spec(2, "region")), 1);
    assertDefaultSpec(ImmutableList.of(spec(1, "region"), spec(2, "id")), 2, "id");
    // Specs are not ordered by id.
    assertDefaultSpec(ImmutableList.of(spec(2, "id"), spec(1, "region")), 1, "region");
    // Dense spec ids.
    assertDefaultSpec(ImmutableList.of(spec(0), spec(1, "region")), 1, "region");
    assertDefaultSpec(ImmutableList.of(spec(0), spec(1, "region")), 0);
  }

  @Test
  public void testNoPartitionSpecs() {
    assertNull(Utils.getDefaultPartitionSpec(mockTable(ImmutableList.of(), 0)));
  }

  @Test
  public void testMissingDefaultPartitionSpec() {
    FeIcebergTable table = mockTable(ImmutableList.of(spec(0)), 5);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> Utils.getDefaultPartitionSpec(table));
    assertTrue(e.getMessage(), e.getMessage().contains(
        "Default partition spec 5 of table " + TABLE_NAME + " not found"));
    assertTrue(e.getMessage(), e.getMessage().contains("Partition spec ids: [0]"));
  }

  /**
   * Returns a mocked FeIcebergTable that wraps 'iceApiTable' like IcebergTable does: its
   * partition specs are loaded from the Iceberg table, and the default partition spec id
   * is the id of the Iceberg table's current spec.
   */
  private static FeIcebergTable wrapIcebergTable(Table iceApiTable) throws Exception {
    FeIcebergTable table = mock(FeIcebergTable.class);
    when(table.getIcebergApiTable()).thenReturn(iceApiTable);
    when(table.getFullName()).thenReturn(TABLE_NAME);
    // Load the specs before stubbing: calling a mock inside thenReturn() is an
    // UnfinishedStubbingException.
    List<IcebergPartitionSpec> specs = Utils.loadPartitionSpecByIceberg(table);
    when(table.getPartitionSpecs()).thenReturn(specs);
    when(table.getPartitionSpec(anyInt())).thenCallRealMethod();
    when(table.getDefaultPartitionSpecId()).thenReturn(iceApiTable.spec().specId());
    return table;
  }

  /**
   * Creates a real Iceberg table whose partition spec ids become non-dense after
   * ExpireSnapshots.cleanExpiredMetadata(true), then evolves it further.
   */
  @Test
  public void testCleanExpiredMetadata() throws Exception {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "region", Types.StringType.get()));
    Table iceTable = catalog.buildTable(TableIdentifier.of("db", "t"), schema)
        .withPartitionSpec(PartitionSpec.unpartitioned())
        .withProperty(TableProperties.FORMAT_VERSION, "2")
        .create();

    // Partition the table before its first write: specs [0, 1], default spec 1.
    iceTable.updateSpec().addField("region").commit();
    assertEquals(ImmutableSet.of(0, 1), iceTable.specs().keySet());
    assertEquals(1, iceTable.spec().specId());

    // Spec 0 is not used by any snapshot (there are none), so it is removed.
    iceTable.expireSnapshots().cleanExpiredMetadata(true).commit();
    iceTable.refresh();
    assertEquals(ImmutableSet.of(1), iceTable.specs().keySet());
    IcebergPartitionSpec defaultSpec =
        Utils.getDefaultPartitionSpec(wrapIcebergTable(iceTable));
    assertEquals(1, defaultSpec.getSpecId());
    assertEquals(ImmutableList.of("region"), fieldNames(defaultSpec));

    // A new spec gets id 2 (max id + 1), so the hole is never filled.
    iceTable.updateSpec().removeField("region").addField("id").commit();
    assertEquals(ImmutableSet.of(1, 2), iceTable.specs().keySet());
    assertEquals(2, iceTable.spec().specId());
    defaultSpec = Utils.getDefaultPartitionSpec(wrapIcebergTable(iceTable));
    assertEquals(2, defaultSpec.getSpecId());
    assertEquals(ImmutableList.of("id"), fieldNames(defaultSpec));

    // Going back to identity(region) reuses spec id 1: specs [1, 2], default spec 1.
    iceTable.updateSpec().removeField("id").addField("region").commit();
    assertEquals(ImmutableSet.of(1, 2), iceTable.specs().keySet());
    assertEquals(1, iceTable.spec().specId());
    FeIcebergTable feTable = wrapIcebergTable(iceTable);
    assertEquals(2, feTable.getPartitionSpecs().size());
    defaultSpec = Utils.getDefaultPartitionSpec(feTable);
    assertEquals(1, defaultSpec.getSpecId());
    assertEquals(ImmutableList.of("region"), fieldNames(defaultSpec));
  }
}

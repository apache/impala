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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TStructField;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;

/**
 * Describes a VARIANT type. Internally represented as a complex type with children
 * for the metadata blob and value blob (both BINARY).
 *
 * For unshredded variants, the children are:
 *   - "metadata" (BINARY): field name dictionary
 *   - "value" (BINARY): encoded variant value
 */
public class VariantType extends Type {
  private final List<StructField> fields_;

  public VariantType(List<StructField> fields) {
    fields_ = fields;
  }

  public VariantType() {
    fields_ = new ArrayList<>();
    fields_.add(new StructField("metadata", Type.BINARY));
    fields_.add(new StructField("value", Type.BINARY));
  }

  public List<StructField> getFields() { return fields_; }

  @Override
  public String toSql(int depth) { return "VARIANT"; }

  /**
   * HMS does not recognize a 'variant' column type, so we store the underlying
   * struct<metadata:binary,value:binary> representation in the metastore (matching
   * Iceberg's Hive catalog / Trino). Impala reads the real VARIANT type from the Iceberg
   * metadata, so the metastore FieldSchema type is only used for Hive interop.
   */
  @Override
  public String toHiveMetastoreType() {
    StringBuilder sb = new StringBuilder("struct<");
    for (int i = 0; i < fields_.size(); ++i) {
      if (i > 0) sb.append(",");
      StructField f = fields_.get(i);
      sb.append(f.getName()).append(":").append(f.getType().toHiveMetastoreType());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  protected String prettyPrint(int lpad) {
    return StringUtils.repeat(' ', lpad) + toSql();
  }

  @Override
  public int getSlotSize() { return 24; }

  @Override
  public boolean equals(Object other) {
    return other instanceof VariantType;
  }

  @Override
  public int hashCode() {
    return "VARIANT".hashCode();
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    node.setType(TTypeNodeType.VARIANT);
    node.setVariant_fields(new ArrayList<>());
    for (StructField field : fields_) {
      TStructField thriftField = new TStructField();
      thriftField.setName(field.getName());
      if (field.getComment() != null) thriftField.setComment(field.getComment());
      node.variant_fields.add(thriftField);
      field.getType().toThrift(container);
    }
  }

  @Override
  public boolean supportsTablePartitioning() { return false; }
}

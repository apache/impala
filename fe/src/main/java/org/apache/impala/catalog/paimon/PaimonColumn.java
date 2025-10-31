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

package org.apache.impala.catalog.paimon;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnDescriptor;

/**
 * Represents a Paimon column.
 *
 * This class extends Column with the Paimon-specific field id. Field ids are used in
 * schema evolution to uniquely identify columns..
 */
public class PaimonColumn extends Column {
  private final int fieldId_;
  // False for required Paimon field, true for optional Paimon field
  private final boolean isNullable_;

  public PaimonColumn(String name, Type type, String comment, int position, int fieldId,
      boolean isNullable) {
    super(name.toLowerCase(), type, comment, position);
    fieldId_ = fieldId;
    isNullable_ = isNullable;
  }

  public PaimonColumn(String name, Type type, String comment, int position, int fieldId) {
    this(name, type, comment, position, fieldId, true);
  }

  public int getFieldId() { return fieldId_; }

  public boolean isNullable() { return isNullable_; }

  @Override
  public TColumn toThrift() {
    TColumn tcol = super.toThrift();
    tcol.setIs_paimon_column(true);
    tcol.setIceberg_field_id(fieldId_);
    tcol.setIs_nullable(isNullable_);
    return tcol;
  }

  @Override
  public TColumnDescriptor toDescriptor() {
    TColumnDescriptor desc = super.toDescriptor();
    desc.setIcebergFieldId(fieldId_);
    return desc;
  }
}

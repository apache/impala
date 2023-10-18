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

import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TVirtualColumnType;

import com.google.common.base.Preconditions;

/**
 * Virtual columns are columns that are not stored by the table, but they can reveal some
 * internal metadata, e.g. input file name of the rows.
 * They can be specific to table types. E.g. INPUT__FILE__NAME is specific to filesystem
 * based tables.
 * Virtual columns are hidden in the sense that they are not included in
 * SELECT * expansions.
 */
public class VirtualColumn extends Column {
  private final TVirtualColumnType virtualColType_;

  // Virtual columns of FileSystem-based tables.
  public static VirtualColumn INPUT_FILE_NAME = new VirtualColumn("INPUT__FILE__NAME",
      Type.STRING, TVirtualColumnType.INPUT_FILE_NAME);
  public static VirtualColumn FILE_POSITION = new VirtualColumn("FILE__POSITION",
      Type.BIGINT, TVirtualColumnType.FILE_POSITION);

  /// Iceberg-related virtual columns.
  public static VirtualColumn PARTITION_SPEC_ID = new VirtualColumn("PARTITION__SPEC__ID",
      Type.INT, TVirtualColumnType.PARTITION_SPEC_ID);
  public static VirtualColumn ICEBERG_PARTITION_SERIALIZED = new
      VirtualColumn("ICEBERG__PARTITION__SERIALIZED", Type.BINARY,
      TVirtualColumnType.ICEBERG_PARTITION_SERIALIZED);
  public static VirtualColumn ICEBERG_DATA_SEQUENCE_NUMBER = new VirtualColumn(
      "ICEBERG__DATA__SEQUENCE__NUMBER",
      Type.BIGINT,
      TVirtualColumnType.ICEBERG_DATA_SEQUENCE_NUMBER);

  public static VirtualColumn getVirtualColumn(TVirtualColumnType virtColType) {
    switch (virtColType) {
      case INPUT_FILE_NAME: return INPUT_FILE_NAME;
      case FILE_POSITION: return FILE_POSITION;
      case PARTITION_SPEC_ID: return PARTITION_SPEC_ID;
      case ICEBERG_PARTITION_SERIALIZED: return ICEBERG_PARTITION_SERIALIZED;
      case ICEBERG_DATA_SEQUENCE_NUMBER: return ICEBERG_DATA_SEQUENCE_NUMBER;
      default: break;
    }
    return null;
  }

  public TVirtualColumnType getVirtualColumnType() { return virtualColType_; }

  private VirtualColumn(String name, Type type, TVirtualColumnType virtualColType) {
    super(name.toLowerCase(), type, 0);
    virtualColType_ = virtualColType;
  }

  @Override
  public boolean isVirtual() { return true; }

  public static VirtualColumn fromThrift(TColumn columnDesc) {
    Preconditions.checkState(columnDesc.isSetVirtual_column_type());
    return new VirtualColumn(columnDesc.getColumnName(),
        Type.fromThrift(columnDesc.getColumnType()),
        columnDesc.getVirtual_column_type());
  }

  public TColumn toThrift() {
    TColumn colDesc = super.toThrift();
    colDesc.setVirtual_column_type(virtualColType_);
    return colDesc;
  }
}

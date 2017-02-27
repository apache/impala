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

import org.apache.impala.thrift.TPrimitiveType;
import com.google.common.collect.Lists;

public enum PrimitiveType {
  INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE),
  // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
  // with all other types.
  NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE),
  BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN),
  TINYINT("TINYINT", 1, TPrimitiveType.TINYINT),
  SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT),
  INT("INT", 4, TPrimitiveType.INT),
  BIGINT("BIGINT", 8, TPrimitiveType.BIGINT),
  FLOAT("FLOAT", 4, TPrimitiveType.FLOAT),
  DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE),
  DATE("DATE", 4, TPrimitiveType.DATE),
  DATETIME("DATETIME", 8, TPrimitiveType.DATETIME),
  // The timestamp structure is 12 bytes, Aligning to 8 bytes makes it 16.
  TIMESTAMP("TIMESTAMP", 16, TPrimitiveType.TIMESTAMP),
  // 8-byte pointer and 4-byte length indicator (12 bytes total).
  // Aligning to 8 bytes so 16 total.
  STRING("STRING", 16, TPrimitiveType.STRING),
  VARCHAR("VARCHAR", 16, TPrimitiveType.VARCHAR),

  // Unsupported scalar type.
  BINARY("BINARY", -1, TPrimitiveType.BINARY),

  // For decimal at the highest precision, the BE uses 16 bytes.
  DECIMAL("DECIMAL", 16, TPrimitiveType.DECIMAL),

  // Fixed length char array.
  CHAR("CHAR", -1, TPrimitiveType.CHAR);

  private final String description_;
  private final int slotSize_;  // size of tuple slot for this type
  private final TPrimitiveType thriftType_;

  private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
    description_ = description;
    slotSize_ = slotSize;
    thriftType_ = thriftType;
  }

  @Override
  public String toString() {
    return description_;
  }

  public static PrimitiveType fromThrift(TPrimitiveType t) {
    switch (t) {
      case INVALID_TYPE: return INVALID_TYPE;
      case NULL_TYPE: return NULL_TYPE;
      case BOOLEAN: return BOOLEAN;
      case TINYINT: return TINYINT;
      case SMALLINT: return SMALLINT;
      case INT: return INT;
      case BIGINT: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case DATE: return DATE;
      case DATETIME: return DATETIME;
      case STRING: return STRING;
      case VARCHAR: return VARCHAR;
      case TIMESTAMP: return TIMESTAMP;
      case CHAR: return CHAR;
      case DECIMAL: return DECIMAL;
      case BINARY: return BINARY;
    }
    return INVALID_TYPE;
  }

  public TPrimitiveType toThrift() { return thriftType_; }

  public static List<TPrimitiveType> toThrift(PrimitiveType[] types) {
    List<TPrimitiveType> result = Lists.newArrayList();
    for (PrimitiveType t: types) {
      result.add(t.toThrift());
    }
    return result;
  }

  public int getSlotSize() { return slotSize_; }
}

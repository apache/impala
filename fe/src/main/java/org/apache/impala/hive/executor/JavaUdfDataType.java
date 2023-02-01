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

package org.apache.impala.hive.executor;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TPrimitiveType;

import com.google.common.base.Preconditions;
import org.apache.impala.util.UnsafeUtil;

// Data types that are supported as return or argument types in Java UDFs.
public enum JavaUdfDataType {
  INVALID_TYPE("INVALID_TYPE", TPrimitiveType.INVALID_TYPE),
  BOOLEAN("BOOLEAN", TPrimitiveType.BOOLEAN),
  BOOLEAN_WRITABLE("BOOLEAN_WRITABLE", TPrimitiveType.BOOLEAN),
  TINYINT("TINYINT", TPrimitiveType.TINYINT),
  BYTE_WRITABLE("BYTE_WRITABLE", TPrimitiveType.TINYINT),
  SMALLINT("SMALLINT", TPrimitiveType.SMALLINT),
  SHORT_WRITABLE("SHORT_WRITABLE", TPrimitiveType.SMALLINT),
  INT("INT", TPrimitiveType.INT),
  INT_WRITABLE("INT_WRITABLE", TPrimitiveType.INT),
  BIGINT("BIGINT", TPrimitiveType.BIGINT),
  LONG_WRITABLE("LONG_WRITABLE", TPrimitiveType.BIGINT),
  FLOAT("FLOAT", TPrimitiveType.FLOAT),
  FLOAT_WRITABLE("FLOAT_WRITABLE", TPrimitiveType.FLOAT),
  DOUBLE("DOUBLE", TPrimitiveType.DOUBLE),
  DOUBLE_WRITABLE("DOUBLE", TPrimitiveType.DOUBLE),
  STRING("STRING", TPrimitiveType.STRING),
  TEXT("TEXT", TPrimitiveType.STRING),
  BYTES_WRITABLE("BYTES_WRITABLE", TPrimitiveType.STRING),
  BYTE_ARRAY("BYTE_ARRAY", TPrimitiveType.STRING);

  public static final int STRING_VALUE_LEN_OFFSET = 8;

  private final String description_;
  private final TPrimitiveType thriftType_;

  private JavaUdfDataType(String description, TPrimitiveType thriftType) {
    description_ = description;
    thriftType_ = thriftType;
  }

  @Override
  public String toString() { return description_; }

  public String getDescription() { return description_; }

  public TPrimitiveType getPrimitiveType() { return thriftType_; }

  public static JavaUdfDataType[] getTypes(Type[] typeArray) {
    JavaUdfDataType[] types = new JavaUdfDataType[typeArray.length];
    for (int i = 0; i < typeArray.length; ++i) {
      types[i] = getType(typeArray[i]);
    }
    return types;
  }

  public static JavaUdfDataType[] getTypes(Class<?>[] typeArray) {
    JavaUdfDataType[] types = new JavaUdfDataType[typeArray.length];
    for (int i = 0; i < typeArray.length; ++i) {
      types[i] = getType(typeArray[i]);
    }
    return types;
  }

  public static JavaUdfDataType getType(Type t) {
    switch (t.getPrimitiveType().toThrift()) {
      case BOOLEAN: return JavaUdfDataType.BOOLEAN_WRITABLE;
      case TINYINT: return JavaUdfDataType.BYTE_WRITABLE;
      case SMALLINT: return JavaUdfDataType.SHORT_WRITABLE;
      case INT: return JavaUdfDataType.INT_WRITABLE;
      case BIGINT: return JavaUdfDataType.LONG_WRITABLE;
      case FLOAT: return JavaUdfDataType.FLOAT_WRITABLE;
      case DOUBLE: return JavaUdfDataType.DOUBLE_WRITABLE;
      case STRING: return JavaUdfDataType.TEXT;
      case BINARY: return JavaUdfDataType.BYTES_WRITABLE;
      default: return null;
    }
  }

  public static JavaUdfDataType getType(ObjectInspector oi) {
    // Only primitive objects are supported currently.
    Preconditions.checkState(oi instanceof PrimitiveObjectInspector);
    PrimitiveObjectInspector primOi = (PrimitiveObjectInspector) oi;
    PrimitiveCategory cat = primOi.getPrimitiveCategory();
    boolean writable = primOi.preferWritable();
    switch (cat) {
      case BOOLEAN:
        return writable ? JavaUdfDataType.BOOLEAN_WRITABLE : JavaUdfDataType.BOOLEAN;
      case BYTE:
        return writable ? JavaUdfDataType.BYTE_WRITABLE : JavaUdfDataType.TINYINT;
      case SHORT:
        return writable ? JavaUdfDataType.SHORT_WRITABLE : JavaUdfDataType.SMALLINT;
      case INT:
        return writable ? JavaUdfDataType.INT_WRITABLE : JavaUdfDataType.INT;
      case LONG:
        return writable ? JavaUdfDataType.LONG_WRITABLE : JavaUdfDataType.BIGINT;
      case FLOAT:
        return writable ? JavaUdfDataType.FLOAT_WRITABLE : JavaUdfDataType.FLOAT;
      case DOUBLE:
        return writable ? JavaUdfDataType.DOUBLE_WRITABLE : JavaUdfDataType.DOUBLE;
      case STRING:
        return writable ? JavaUdfDataType.TEXT : JavaUdfDataType.STRING;
      case BINARY:
        return writable ? JavaUdfDataType.BYTES_WRITABLE : JavaUdfDataType.BYTE_ARRAY;
      default:
        return null;
    }
  }

  public static JavaUdfDataType getType(Class<?> c) {
    if (c == BooleanWritable.class) {
      return JavaUdfDataType.BOOLEAN_WRITABLE;
    } else if (c == boolean.class || c == Boolean.class) {
      return JavaUdfDataType.BOOLEAN;
    } else if (c == ByteWritable.class) {
      return JavaUdfDataType.BYTE_WRITABLE;
    } else if (c == byte.class || c == Byte.class) {
      return JavaUdfDataType.TINYINT;
    } else if (c == ShortWritable.class) {
      return JavaUdfDataType.SHORT_WRITABLE;
    } else if (c == short.class || c == Short.class) {
      return JavaUdfDataType.SMALLINT;
    } else if (c == IntWritable.class) {
      return JavaUdfDataType.INT_WRITABLE;
    } else if (c == int.class || c == Integer.class) {
      return JavaUdfDataType.INT;
    } else if (c == LongWritable.class) {
      return JavaUdfDataType.LONG_WRITABLE;
    } else if (c == long.class || c == Long.class) {
      return JavaUdfDataType.BIGINT;
    } else if (c == FloatWritable.class) {
      return JavaUdfDataType.FLOAT_WRITABLE;
    } else if (c == float.class || c == Float.class) {
      return JavaUdfDataType.FLOAT;
    } else if (c == DoubleWritable.class) {
      return JavaUdfDataType.DOUBLE_WRITABLE;
    } else if (c == double.class || c == Double.class) {
      return JavaUdfDataType.DOUBLE;
    } else if (c == byte[].class) {
      return JavaUdfDataType.BYTE_ARRAY;
    } else if (c == BytesWritable.class) {
      return JavaUdfDataType.BYTES_WRITABLE;
    } else if (c == Text.class) {
      return JavaUdfDataType.TEXT;
    } else if (c == String.class) {
      return JavaUdfDataType.STRING;
    }
    return JavaUdfDataType.INVALID_TYPE;
  }

  public static boolean isSupported(Type t) {
    if (TPrimitiveType.INVALID_TYPE == t.getPrimitiveType().toThrift()) {
      return false;
    }

    // While BYTES_WRITABLE and BYTE_ARRAY maps to STRING to keep compatibility,
    // BINARY is also accepted (IMPALA-11340).
    if (t.isBinary()) return true;

    for (JavaUdfDataType javaType : JavaUdfDataType.values()) {
      if (javaType.getPrimitiveType() == t.getPrimitiveType().toThrift()) {
        return true;
      }
    }
    return false;
  }

  public boolean isCompatibleWith(TPrimitiveType t) {
    if (t == getPrimitiveType()) return true;
    if (t == TPrimitiveType.BINARY) {
      // While BYTES_WRITABLE and BYTE_ARRAY maps to STRING to keep compatibility,
      // BINARY is also accepted (IMPALA-11340).
      if (this == BYTE_ARRAY || this == BYTES_WRITABLE) return true;
    }
    return false;
  }

  // Returns a backend-allocated string as a Java byte array
  public static byte[] loadStringValueFromNativeHeap(long ptr) {
    int length = UnsafeUtil.UNSAFE.getInt(ptr + STRING_VALUE_LEN_OFFSET);
    byte[] buffer = new byte[length];
    long srcPtr = UnsafeUtil.UNSAFE.getLong(ptr);
    UnsafeUtil.Copy(buffer, 0, srcPtr, length);
    return buffer;
  }
}

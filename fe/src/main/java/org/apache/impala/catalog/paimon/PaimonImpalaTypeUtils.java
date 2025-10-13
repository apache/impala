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

package org.apache.impala.catalog.paimon;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.List;
import java.util.stream.Collectors;

/** Utils for converting types related classes between Paimon and Impala. */
public class PaimonImpalaTypeUtils {
  /**
   * Convert paimon data type {@link DataType} to impala data type {@link Type}.
   *
   * @param logicalType paimon data type.
   * @return impala type info.
   */
  public static Type toImpalaType(DataType logicalType) {
    return logicalType.accept(PaimonToImpalaTypeVisitor.INSTANCE);
  }

  /**
   * Convert impala data type {@link Type} to paimon data type {@link DataType}.
   *
   * @param Type hive type info
   * @return paimon data type
   */
  public static DataType fromImpalaType(Type Type) {
    return ImpalaToPaimonTypeVisitor.visit(Type);
  }

  private static class PaimonToImpalaTypeVisitor extends DataTypeDefaultVisitor<Type> {
    private static final PaimonToImpalaTypeVisitor INSTANCE =
        new PaimonToImpalaTypeVisitor();

    @Override
    public Type visit(BooleanType booleanType) {
      return Type.BOOLEAN;
    }

    @Override
    public Type visit(TinyIntType tinyIntType) {
      return Type.TINYINT;
    }

    @Override
    public Type visit(SmallIntType smallIntType) {
      return Type.SMALLINT;
    }

    @Override
    public Type visit(IntType intType) {
      return Type.INT;
    }

    @Override
    public Type visit(BigIntType bigIntType) {
      return Type.BIGINT;
    }

    @Override
    public Type visit(FloatType floatType) {
      return Type.FLOAT;
    }

    @Override
    public Type visit(DoubleType doubleType) {
      return Type.DOUBLE;
    }

    @Override
    public Type visit(DecimalType decimalType) {
      return ScalarType.createDecimalType(
          decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public Type visit(CharType charType) {
      if (charType.getLength() > HiveChar.MAX_CHAR_LENGTH) {
        return Type.STRING;
      } else {
        return ScalarType.createCharType(charType.getLength());
      }
    }

    @Override
    public Type visit(VarCharType varCharType) {
      if (varCharType.getLength() > HiveVarchar.MAX_VARCHAR_LENGTH) {
        return Type.STRING;
      } else {
        return ScalarType.createVarcharType(varCharType.getLength());
      }
    }

    @Override
    public Type visit(BinaryType binaryType) {
      return Type.BINARY;
    }

    @Override
    public Type visit(VarBinaryType varBinaryType) {
      return Type.BINARY;
    }

    @Override
    public Type visit(DateType dateType) {
      return Type.DATE;
    }

    @Override
    public Type visit(TimestampType timestampType) {
      return Type.TIMESTAMP;
    }

    @Override
    public Type visit(LocalZonedTimestampType localZonedTimestampType) {
        return Type.TIMESTAMP;
    }

    @Override
    public Type visit(ArrayType arrayType) {
      DataType elementType = arrayType.getElementType();
      return new org.apache.impala.catalog.ArrayType(elementType.accept(this));
    }

    @Override
    public Type visit(MapType mapType) {
      return new org.apache.impala.catalog.MapType(
          mapType.getKeyType().accept(this), mapType.getValueType().accept(this));
    }

    @Override
    public Type visit(RowType rowType) {
      List<StructField> structFields =
          rowType.getFields()
              .stream()
              .map(dataField
                  -> new StructField(
                      dataField.name().toLowerCase(), dataField.type().accept(this)))
              .collect(Collectors.toList());

      return new StructType(structFields);
    }

    @Override
    protected Type defaultMethod(org.apache.paimon.types.DataType dataType) {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  private static class ImpalaToPaimonTypeVisitor {
    static DataType visit(Type type) {
      return visit(type, new ImpalaToPaimonTypeVisitor());
    }

    static DataType visit(Type type, ImpalaToPaimonTypeVisitor visitor) {
      if (type.isStructType()) {
        StructType structTypeInfo = (StructType) type;
        List<StructField> fields = structTypeInfo.getFields();
        RowType.Builder builder = RowType.builder();
        for (StructField field : fields) {
          builder.field(field.getName(), visit(field.getType(), visitor));
        }
        return builder.build();
      } else if (type.isMapType()) {
        org.apache.impala.catalog.MapType mapTypeInfo =
            (org.apache.impala.catalog.MapType) type;
        return DataTypes.MAP(visit(mapTypeInfo.getKeyType(), visitor),
            visit(mapTypeInfo.getValueType(), visitor));
      } else if (type.isArrayType()) {
        org.apache.impala.catalog.ArrayType listTypeInfo =
            (org.apache.impala.catalog.ArrayType) type;
        return DataTypes.ARRAY(visit(listTypeInfo.getItemType(), visitor));
      } else {
        return visitor.atomic(type);
      }
    }

    public DataType atomic(Type atomic) {
      PrimitiveType primitiveType = atomic.getPrimitiveType();
      switch (primitiveType) {
        case BOOLEAN: return DataTypes.BOOLEAN();
        case TINYINT: return DataTypes.TINYINT();
        case SMALLINT: return DataTypes.SMALLINT();
        case INT: return DataTypes.INT();
        case BIGINT: return DataTypes.BIGINT();
        case FLOAT: return DataTypes.FLOAT();
        case DOUBLE: return DataTypes.DOUBLE();
        case DECIMAL:
          ScalarType scalarType = (ScalarType) atomic;
          return DataTypes.DECIMAL(
              scalarType.decimalPrecision(), scalarType.decimalScale());
        case CHAR: return DataTypes.CHAR(((ScalarType) atomic).getLength());
        case VARCHAR: return DataTypes.VARCHAR(((ScalarType) atomic).getLength());
        case STRING: return DataTypes.STRING();
        case DATE: return DataTypes.DATE();
        case TIMESTAMP: return DataTypes.TIMESTAMP();
        case BINARY: return DataTypes.BINARY(BinaryType.MAX_LENGTH);
        default:
          throw new UnsupportedOperationException(
              "Not a supported type: " + atomic);
      }
    }
  }

  /**
   * Returns true if the primitive type is supported.
   */
  public static boolean isSupportedPrimitiveType(PrimitiveType primitiveType) {
    Preconditions.checkNotNull(primitiveType);
    switch (primitiveType) {
      case BIGINT:
      case INT:
      case SMALLINT:
      case TINYINT:
      case DOUBLE:
      case FLOAT:
      case BOOLEAN:
      case STRING:
      case TIMESTAMP:
      case DECIMAL:
      case DATE:
      case BINARY:
      case CHAR:
      case DATETIME:
      case VARCHAR: return true;
      default: return false;
    }
  }

  /**
   * Returns true if the column type is supported.
   */
  public static boolean isSupportedColumnType(Type colType) {
    Preconditions.checkNotNull(colType);
    return isSupportedPrimitiveType(colType.getPrimitiveType());
  }
}

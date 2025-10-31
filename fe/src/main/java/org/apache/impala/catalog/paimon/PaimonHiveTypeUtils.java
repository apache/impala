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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.paimon.hive.LocalZonedTimestampTypeUtils;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
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
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

/**
 * Utils for paimon and hive Type conversions, the class is from
 * org.apache.paimon.hive.HiveTypeUtils, refactor to fix the
 * following incompatible conversion issue:
 * paimon type ${@link LocalZonedTimestampType} will convert to
 * ${@link org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo}
 * however, it is not supported in impala, TableLoadingException
 * will raise while loading the table in method:
 * ${@link org.apache.impala.catalog.FeCatalogUtils#parseColumnType}
 * To fix the issue LocalZonedTimestampType will be converted to
 * hive timestamp type.
 */
public class PaimonHiveTypeUtils {

  public static TypeInfo toTypeInfo(DataType logicalType) {
    return (TypeInfo)logicalType.accept(PaimonToHiveTypeVisitor.INSTANCE);
  }

  public static DataType toPaimonType(String type) {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
    return toPaimonType(typeInfo);
  }

  public static DataType toPaimonType(TypeInfo typeInfo) {
    return HiveToPaimonTypeVisitor.visit(typeInfo);
  }

  private static class PaimonToHiveTypeVisitor extends
      DataTypeDefaultVisitor<TypeInfo> {
    private static final PaimonToHiveTypeVisitor INSTANCE =
        new PaimonToHiveTypeVisitor();

    public TypeInfo visit(BooleanType booleanType) {
      return TypeInfoFactory.booleanTypeInfo;
    }

    public TypeInfo visit(TinyIntType tinyIntType) {
      return TypeInfoFactory.byteTypeInfo;
    }

    public TypeInfo visit(SmallIntType smallIntType) {
      return TypeInfoFactory.shortTypeInfo;
    }

    public TypeInfo visit(IntType intType) {
      return TypeInfoFactory.intTypeInfo;
    }

    public TypeInfo visit(BigIntType bigIntType) {
      return TypeInfoFactory.longTypeInfo;
    }

    public TypeInfo visit(FloatType floatType) {
      return TypeInfoFactory.floatTypeInfo;
    }

    public TypeInfo visit(DoubleType doubleType) {
      return TypeInfoFactory.doubleTypeInfo;
    }

    public TypeInfo visit(DecimalType decimalType) {
      return TypeInfoFactory.getDecimalTypeInfo(decimalType.getPrecision(),
          decimalType.getScale());
    }

    public TypeInfo visit(CharType charType) {
      return (TypeInfo)(charType.getLength() > 255
          ? TypeInfoFactory.stringTypeInfo
          : TypeInfoFactory.getCharTypeInfo(charType.getLength()));
    }

    public TypeInfo visit(VarCharType varCharType) {
      return (TypeInfo)(varCharType.getLength() > 65535
          ? TypeInfoFactory.stringTypeInfo
          : TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength()));
    }

    public TypeInfo visit(BinaryType binaryType) {
      return TypeInfoFactory.binaryTypeInfo;
    }

    public TypeInfo visit(VarBinaryType varBinaryType) {
      return TypeInfoFactory.binaryTypeInfo;
    }

    public TypeInfo visit(DateType dateType) {
      return TypeInfoFactory.dateTypeInfo;
    }

    public TypeInfo visit(TimeType timeType) {
      return TypeInfoFactory.stringTypeInfo;
    }

    public TypeInfo visit(TimestampType timestampType) {
      return TypeInfoFactory.timestampTypeInfo;
    }
    // changed to timestamp without local timezone.
    public TypeInfo visit(LocalZonedTimestampType localZonedTimestampType) {
      return TypeInfoFactory.timestampTypeInfo;
    }

    public TypeInfo visit(ArrayType arrayType) {
      DataType elementType = arrayType.getElementType();
      return TypeInfoFactory.getListTypeInfo((TypeInfo)elementType.accept(this));
    }

    public TypeInfo visit(MultisetType multisetType) {
      return TypeInfoFactory.getMapTypeInfo((TypeInfo)multisetType
          .getElementType()
          .accept(this), TypeInfoFactory.intTypeInfo);
    }

    public TypeInfo visit(MapType mapType) {
      return TypeInfoFactory.getMapTypeInfo((TypeInfo)mapType
          .getKeyType().accept(this), (TypeInfo)mapType.getValueType()
          .accept(this));
    }

    public TypeInfo visit(RowType rowType) {
      List<String> fieldNames = (List)rowType.getFields().stream()
          .map(DataField::name)
          .collect(Collectors.toList());
      List<TypeInfo> typeInfos = (List)rowType.getFields().stream()
          .map(DataField::type).map((type)
              -> (TypeInfo)type.accept(this))
          .collect(Collectors.toList());
      return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    public TypeInfo visit(VariantType variantType) {
      List<String> fieldNames = Arrays.asList("value", "metadata");
      List<TypeInfo> typeInfos = Arrays.asList(TypeInfoFactory.binaryTypeInfo,
          TypeInfoFactory.binaryTypeInfo);
      return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    protected TypeInfo defaultMethod(DataType dataType) {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  private static class HiveToPaimonTypeVisitor {
    static DataType visit(TypeInfo type) {
      return visit(type, new HiveToPaimonTypeVisitor());
    }

    static DataType visit(TypeInfo type, HiveToPaimonTypeVisitor visitor) {
      if (type instanceof StructTypeInfo) {
        StructTypeInfo structTypeInfo = (StructTypeInfo)type;
        ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> typeInfos = structTypeInfo
            .getAllStructFieldTypeInfos();
        RowType.Builder builder = RowType.builder();
        for(int i = 0; i < fieldNames.size(); ++i) {
          builder.field((String)fieldNames.get(i),
              visit((TypeInfo)typeInfos.get(i), visitor));
        }
        return builder.build();
      } else if (type instanceof MapTypeInfo) {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
        return DataTypes.MAP(visit(mapTypeInfo.getMapKeyTypeInfo(), visitor),
            visit(mapTypeInfo.getMapValueTypeInfo(), visitor));
      } else if (type instanceof ListTypeInfo) {
        ListTypeInfo listTypeInfo = (ListTypeInfo) type;
        return DataTypes.ARRAY(visit(listTypeInfo.getListElementTypeInfo(), visitor));
      } else {
        return visitor.atomic(type);
      }
    }

    public DataType atomic(TypeInfo atomic) {
      if (LocalZonedTimestampTypeUtils.isHiveLocalZonedTimestampType(atomic)) {
        return DataTypes.TIMESTAMP_MILLIS();
      } else if (TypeInfoFactory.booleanTypeInfo.equals(atomic)) {
        return DataTypes.BOOLEAN();
      } else if (TypeInfoFactory.byteTypeInfo.equals(atomic)) {
        return DataTypes.TINYINT();
      } else if (TypeInfoFactory.shortTypeInfo.equals(atomic)) {
        return DataTypes.SMALLINT();
      } else if (TypeInfoFactory.intTypeInfo.equals(atomic)) {
        return DataTypes.INT();
      } else if (TypeInfoFactory.longTypeInfo.equals(atomic)) {
        return DataTypes.BIGINT();
      } else if (TypeInfoFactory.floatTypeInfo.equals(atomic)) {
        return DataTypes.FLOAT();
      } else if (TypeInfoFactory.doubleTypeInfo.equals(atomic)) {
        return DataTypes.DOUBLE();
      } else if (atomic instanceof DecimalTypeInfo) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)atomic;
        return DataTypes.DECIMAL(decimalTypeInfo.getPrecision(),
            decimalTypeInfo.getScale());
      } else if (atomic instanceof CharTypeInfo) {
        return DataTypes.CHAR(((CharTypeInfo)atomic).getLength());
      } else if (atomic instanceof VarcharTypeInfo) {
        return DataTypes.VARCHAR(((VarcharTypeInfo)atomic).getLength());
      } else if (TypeInfoFactory.stringTypeInfo.equals(atomic)) {
        return DataTypes.VARCHAR(Integer.MAX_VALUE);
      } else if (TypeInfoFactory.binaryTypeInfo.equals(atomic)) {
        return DataTypes.VARBINARY(Integer.MAX_VALUE);
      } else if (TypeInfoFactory.dateTypeInfo.equals(atomic)) {
        return DataTypes.DATE();
      } else if (TypeInfoFactory.timestampTypeInfo.equals(atomic)) {
        return DataTypes.TIMESTAMP_MILLIS();
      } else {
        throw new UnsupportedOperationException("Not a supported type: "
            + atomic.getTypeName());
      }
    }
  }
}

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

package org.apache.impala.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.util.FileAnalysisUtil;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;

import com.google.common.base.Preconditions;

/**
 * Provides a helper function (extract()) which extracts the Impala schema from a given
 * ORC file. Details of the ORC types:
 * https://orc.apache.org/docs/types.html
 */
public class OrcSchemaExtractor {
  private final static String ERROR_MSG =
      "Failed to convert ORC type\n%s\nto an Impala %s type:\n%s\n";

  /**
   * Validates the path and loads the ORC schema of the file. The ORC schema is also an
   * ORC type (TypeDescription), represented as a struct.
   */
  private static TypeDescription loadOrcSchema(Path pathToFile) throws AnalysisException {
    FileAnalysisUtil.CheckIfFile(pathToFile);
    Reader reader = null;
    try {
      reader = OrcFile.createReader(pathToFile,
          new ReaderOptions(FileSystemUtil.getConfiguration()));
    } catch (IOException e) {
      // OrcFile.createReader throws IOException in case of any failure, including trying
      // to open a non-ORC file.
      throw new AnalysisException("Failed to open file as an ORC file: " + e);
    }
    return reader.getSchema();
  }

  /**
   * Converts a primitive ORC type to an Impala Type.
   */
  static private Type convertPrimitiveOrcType(TypeDescription type) {
    Category category = type.getCategory();
    Preconditions.checkState(category.isPrimitive() ||
                             category.equals(Category.TIMESTAMP_INSTANT)); // ORC-790
    switch (category) {
      case BINARY: return Type.STRING;
      case BOOLEAN: return Type.BOOLEAN;
      case BYTE: return Type.TINYINT;
      case CHAR: return ScalarType.createCharType(type.getMaxLength());
      case DATE: return Type.DATE;
      case DECIMAL:
        return ScalarType.createDecimalType(type.getPrecision(), type.getScale());
      case DOUBLE: return Type.DOUBLE;
      case FLOAT: return Type.FLOAT;
      case INT: return Type.INT;
      case LONG: return Type.BIGINT;
      case SHORT: return Type.SMALLINT;
      case STRING: return Type.STRING;
      case TIMESTAMP: return Type.TIMESTAMP;
      case TIMESTAMP_INSTANT: return Type.TIMESTAMP;
      case VARCHAR: return ScalarType.createVarcharType(type.getMaxLength());
      default:
        Preconditions.checkState(false,
            "Unexpected ORC primitive type: " + category.getName());
        return null;
    }
  }

  /**
   * Converts an ORC list type to an Impala array Type. An ORC list contains one child,
   * the TypeDescription of the elements.
   */
  private static ArrayType convertArray(TypeDescription listType)
      throws AnalysisException {
    Preconditions.checkState(listType.getChildren().size() == 1);
    return new ArrayType(convertOrcType(listType.getChildren().get(0)));
  }

  /**
   * Converts an ORC map type to an Impala map Type. An ORC map contains two children,
   * the TypeDescriptions for the keys and values.
   */
  private static MapType convertMap(TypeDescription mapType) throws AnalysisException {
    // ORC maps have two children, one for the keys, one for the values.
    Preconditions.checkState(mapType.getChildren().size() == 2);

    TypeDescription key = mapType.getChildren().get(0);
    TypeDescription value = mapType.getChildren().get(1);

    if (!key.getCategory().isPrimitive()) {
      throw new AnalysisException(String.format(ERROR_MSG, mapType.toString(), "MAP",
          "The key type of the MAP type must be primitive."));
    }

    return new MapType(convertOrcType(key), convertOrcType(value));
  }

  /**
   * Converts an ORC struct type to an Impala struct Type.
   */
  private static StructType convertStruct(TypeDescription structType)
      throws AnalysisException {
    List<StructField> structFields = new ArrayList<>();
    List<String> fieldNames = structType.getFieldNames();
    List<TypeDescription> subTypes = structType.getChildren();
    Preconditions.checkState(subTypes.size() == fieldNames.size());
    for (int i = 0; i < subTypes.size(); i++) {
      StructField f = new StructField(fieldNames.get(i), convertOrcType(subTypes.get(i)));
      structFields.add(f);
    }
    return new StructType(structFields);
  }

  /**
   * Converts a non-primitive ORC type to an Impala Type.
   */
  static private Type convertComplexOrcType(TypeDescription type)
      throws AnalysisException {
    Category category = type.getCategory();
    Preconditions.checkState(!category.isPrimitive());

    switch (category) {
      case LIST: return convertArray(type);
      case MAP: return convertMap(type);
      case STRUCT: return convertStruct(type);
      case UNION:
        throw new AnalysisException(
            "Unsupported ORC type UNION for field " + category.getName());
      default:
        Preconditions.checkState(false,
            "Unexpected ORC primitive type: " + category.getName());
        return null;
    }
  }

  /**
   * Converts an ORC type to an Impala Type.
   */
  static private Type convertOrcType(TypeDescription type) throws AnalysisException {
    Category category = type.getCategory();
    // TIMESTAMP_INSTANT is wrongly defined as a compound type (ORC-790).
    if (category.isPrimitive() || category.equals(Category.TIMESTAMP_INSTANT)) {
      return convertPrimitiveOrcType(type);
    } else {
      return convertComplexOrcType(type);
    }
  }

  /**
   * Parses an ORC file stored in HDFS and returns the corresponding Impala schema.
   * This fails with an analysis exception if any errors occur reading the file,
   * parsing the ORC schema, or if the ORC types cannot be represented in Impala.
   */
  static public List<ColumnDef> extract(HdfsUri location) throws AnalysisException {
    List<ColumnDef> schema = new ArrayList<>();
    TypeDescription orcSchema = loadOrcSchema(location.getPath()); // Returns a STRUCT.
    List<TypeDescription> subTypes = orcSchema.getChildren();
    List<String> fieldNames = orcSchema.getFieldNames();
    Preconditions.checkState(subTypes.size() == fieldNames.size());
    for (int i = 0; i < subTypes.size(); i++) {
      TypeDescription orcType = subTypes.get(i);
      Type type = convertOrcType(orcType);
      Preconditions.checkNotNull(type);
      String colName = fieldNames.get(i);
      Map<ColumnDef.Option, Object> option = new HashMap<>();
      option.put(ColumnDef.Option.COMMENT, "Inferred from ORC file.");
      schema.add(new ColumnDef(colName, new TypeDef(type), option));
    }
    return schema;
  }
}

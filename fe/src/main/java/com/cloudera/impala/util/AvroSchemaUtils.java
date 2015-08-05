// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;

import com.cloudera.impala.analysis.ColumnDef;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Contains utility functions for dealing with Avro schemas.
 */
public class AvroSchemaUtils {

  /**
   * Gets an Avro table's JSON schema from the list of given table property search
   * locations. The schema may be specified as a string literal or provided as a
   * Hadoop FileSystem or http URL that points to the schema. Apart from ensuring
   * that the JSON schema is not SCHEMA_NONE, this function does not perform any
   * additional validation on the returned string (e.g., it may not be a valid
   * schema). Returns the Avro schema or null if none was specified in the search
   * locations. Throws an AnalysisException if a schema was specified, but could not
   * be retrieved, e.g., because of an invalid URL.
   */
  public static String getAvroSchema(List<Map<String, String>> schemaSearchLocations)
      throws AnalysisException {
    String url = null;
    // Search all locations and break out on the first valid schema found.
    for (Map<String, String> schemaLocation: schemaSearchLocations) {
      if (schemaLocation == null) continue;

      String literal =
          schemaLocation.get(
              AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
      if (literal != null && !literal.equals(AvroSerdeUtils.SCHEMA_NONE)) return literal;

      url = schemaLocation.get(
          AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName());
      if (url != null && !url.equals(AvroSerdeUtils.SCHEMA_NONE)) {
        url = url.trim();
        break;
      }
    }
    if (url == null) return null;

    String schema = null;
    InputStream urlStream = null;
    try {
      // TODO: Add support for https:// here.
      if (url.toLowerCase().startsWith("http://")) {
        urlStream = new URL(url).openStream();
        schema = IOUtils.toString(urlStream);
      } else {
        Path path = new Path(url);
        FileSystem fs = null;
        fs = path.getFileSystem(FileSystemUtil.getConfiguration());
        StringBuilder errorMsg = new StringBuilder();
        if (!FileSystemUtil.isPathReachable(path, fs, errorMsg)) {
          throw new AnalysisException(String.format(
              "Invalid avro.schema.url: %s. %s", url, errorMsg));
        }
        schema = FileSystemUtil.readFile(path);
      }
    } catch (AnalysisException e) {
      throw e;
    } catch (IOException e) {
      throw new AnalysisException(String.format(
          "Failed to read Avro schema at: %s. %s ", url, e.getMessage()));
    } catch (Exception e) {
      throw new AnalysisException(String.format(
          "Invalid avro.schema.url: %s. %s", url, e.getMessage()));
    } finally {
      if (urlStream != null) IOUtils.closeQuietly(urlStream);
    }
    return schema;
  }

  /**
   * Reconciles differences in names/types between the given list of column definitions
   * and the column definitions corresponding to an Avro Schema. Populates 'warning'
   * if there are inconsistencies between the column definitions and the Avro schema,
   * Returns the reconciled column definitions according to the following conflict
   * resolution policy:
   *
   * Mismatched number of columns -> Prefer Avro columns.
   * Mismatched name/type -> Prefer Avro column, except:
   *   A CHAR/VARCHAR column definition maps to an Avro STRING, and is preserved
   *   as a CHAR/VARCHAR in the reconciled schema.
   *
   * Behavior for TIMESTAMP:
   * A TIMESTAMP column definition maps to an Avro STRING and is presented as a STRING
   * in the reconciled schema, because Avro has no binary TIMESTAMP representation.
   * As a result, no Avro table may have a TIMESTAMP column.
   */
  public static List<ColumnDef> reconcileSchemas(
      List<ColumnDef> colDefs, List<ColumnDef> avroCols, StringBuilder warning) {
    if (colDefs.size() != avroCols.size()) {
      warning.append(String.format(
          "Ignoring column definitions in favor of Avro schema.\n" +
          "The Avro schema has %s column(s) but %s column definition(s) were given.",
           avroCols.size(), colDefs.size()));
      return avroCols;
    }

    List<ColumnDef> result = Lists.newArrayListWithCapacity(colDefs.size());
    for (int i = 0; i < avroCols.size(); ++i) {
      ColumnDef colDef = colDefs.get(i);
      ColumnDef avroCol = avroCols.get(i);
      Preconditions.checkNotNull(colDef.getType());
      Preconditions.checkNotNull(avroCol.getType());

      // A CHAR/VARCHAR column definition maps to an Avro STRING, and is preserved
      // as a CHAR/VARCHAR in the reconciled schema.
      if ((colDef.getType().isStringType() && avroCol.getType().isStringType())) {
        Preconditions.checkState(
            avroCol.getType().getPrimitiveType() == PrimitiveType.STRING);
        result.add(colDef);
      } else {
        result.add(avroCol);
      }

      // Populate warning string if there are name and/or type inconsistencies.
      if (!colDef.getColName().equals(avroCol.getColName()) ||
          !colDef.getType().equals(avroCol.getType())) {
        if (warning.length() == 0) {
          // Add warning preamble for the first mismatch.
          warning.append("Resolved the following name and/or type inconsistencies " +
              "between the column definitions and the Avro schema.\n");
        }
        warning.append(String.format("Column definition at position %s:  %s %s\n",
            i, colDefs.get(i).getColName(), colDefs.get(i).getType().toSql()));
        warning.append(String.format("Avro schema column at position %s: %s %s\n",
            i, avroCols.get(i).getColName(), avroCols.get(i).getType().toSql()));
        warning.append(String.format("Resolution at position %s: %s %s\n",
            i, result.get(i).getColName(), result.get(i).getType().toSql()));
      }
    }
    Preconditions.checkState(result.size() == avroCols.size());
    Preconditions.checkState(result.size() == colDefs.size());
    return result;
  }

  /**
   * Sets the comment of each column definition to 'from deserializer' if not already
   * set. The purpose of this function is to provide behavioral consistency with
   * Hive ('deserializer' is not applicable to Impala) with respect to column comments
   * set for Avro tables.
   */
  public static void setFromSerdeComment(List<ColumnDef> colDefs) {
    for (ColumnDef colDef: colDefs) {
      if (Strings.isNullOrEmpty(colDef.getComment())) {
        colDef.setComment("from deserializer");
      }
    }
  }
}

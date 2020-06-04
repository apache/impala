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

package org.apache.impala.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Contains utility functions for dealing with Avro schemas.
 */
public abstract class AvroSchemaUtils {
  private final static Logger LOG = LoggerFactory.getLogger(AvroSchemaUtils.class);

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
      } else if (!BackendConfig.INSTANCE.disableCatalogDataOpsDebugOnly()) {
        Path path = new Path(url);
        FileSystem fs = null;
        fs = path.getFileSystem(FileSystemUtil.getConfiguration());
        if (!fs.exists(path)) {
          throw new AnalysisException(String.format(
              "Invalid avro.schema.url: %s. Path does not exist.", url));
        }
        schema = FileSystemUtil.readFile(path);
      } else {
        LOG.info(String.format(
            "Avro schema, %s, not loaded from fs: catalog data ops disabled.", url));
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
   * Reconcile the schema in 'msTbl' with the Avro schema specified in 'avroSchema'.
   *
   * See {@link AvroSchemaUtils#reconcileSchemas(List, List, StringBuilder) for
   * details.
   */
  public static List<FieldSchema> reconcileAvroSchema(
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      String avroSchema) throws AnalysisException {
    Preconditions.checkNotNull(msTbl);
    Preconditions.checkNotNull(avroSchema);

    // Generate new FieldSchemas from the Avro schema. This step reconciles
    // differences in the column definitions and the Avro schema. For
    // Impala-created tables this step is not necessary because the same
    // resolution is done during table creation. But Hive-created tables
    // store the original column definitions, and not the reconciled ones.
    List<ColumnDef> colDefs =
        ColumnDef.createFromFieldSchemas(msTbl.getSd().getCols());
    List<ColumnDef> avroCols = AvroSchemaParser.parse(avroSchema);
    StringBuilder warning = new StringBuilder();
    List<ColumnDef> reconciledColDefs =
        AvroSchemaUtils.reconcileSchemas(colDefs, avroCols, warning);
    if (warning.length() != 0) {
      LOG.warn(String.format("Warning while loading table %s.%s:\n%s",
          msTbl.getDbName(), msTbl.getTableName(), warning.toString()));
    }
    AvroSchemaUtils.setFromSerdeComment(reconciledColDefs);
    return ColumnDef.toFieldSchemas(reconciledColDefs);
  }


  /**
   * Reconciles differences in names/types between the given list of column definitions
   * and the column definitions corresponding to an Avro Schema. Populates 'warning'
   * if there are inconsistencies between the column definitions and the Avro schema,
   * Returns the reconciled column definitions according to the following conflict
   * resolution policy:
   *
   * Mismatched number of columns -> Prefer Avro columns.
   * Always prefer Avro schema except for column type CHAR/VARCHAR/STRING:
   *   A CHAR/VARCHAR/STRING column definition maps to an Avro STRING. The reconciled
   *   column will preserve the type in the column definition but use the column name
   *   and comment from the Avro schema.
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

      // A CHAR/VARCHAR/STRING column definition maps to an Avro STRING, and is preserved
      // as a CHAR/VARCHAR/STRING in the reconciled schema. Column name and comment
      // are taken from the Avro schema.
      if ((colDef.getType().isStringType() && avroCol.getType().isStringType())) {
        Preconditions.checkState(
            avroCol.getType().getPrimitiveType() == PrimitiveType.STRING
            || avroCol.getType().isBinary());
        Map<ColumnDef.Option, Object> option = Maps.newHashMap();
        String comment = avroCol.getComment();
        if (comment != null) option.put(ColumnDef.Option.COMMENT, comment);
        ColumnDef reconciledColDef = new ColumnDef(
            avroCol.getColName(), colDef.getTypeDef(), option);
        try {
          reconciledColDef.analyze(null);
        } catch (AnalysisException e) {
          Preconditions.checkNotNull(
              null, "reconciledColDef.analyze() should never throw.");
        }
        result.add(reconciledColDef);
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

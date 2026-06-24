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
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Contains utility functions for dealing with Avro schemas.
 */
public abstract class AvroSchemaUtils {
  private final static Logger LOG = LoggerFactory.getLogger(AvroSchemaUtils.class);

  private static Pair<String, String> getAvroSchemaLiteralOrUrl(
      List<Map<String, String>> avroSchemaLocs) {
    for (Map<String, String> schemaLocation: avroSchemaLocs) {
      if (schemaLocation == null) continue;

      String literal = schemaLocation.get(
          AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
      if (literal != null && !literal.equals(AvroSerdeUtils.SCHEMA_NONE)) {
        return new Pair<>(literal, null);
      }

      String url = schemaLocation.get(
          AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName());
      if (url != null && !url.equals(AvroSerdeUtils.SCHEMA_NONE)) {
        return new Pair<>(null, url.trim());
      }
    }
    return new Pair<>(null, null);
  }

  /**
   * Analyzes the given Avro schema search locations and registers a privilege request
   * for the URL if one is found. The actual fetch and validation of the schema are
   * deferred to CatalogOpExecutor after authorization.
   *
   * If a schema literal is found, parses and fills avroCols to reconcile.
   * Returns true if a URL schema was found in the search locations, false otherwise.
   */
  public static boolean analyzeAvroSchema(
      Analyzer analyzer, TableName tblName, List<Map<String, String>> avroSchemaLocs,
      List<ColumnDef> colDefs) throws AnalysisException {
    Pair<String, String> literalOrUrl = getAvroSchemaLiteralOrUrl(avroSchemaLocs);
    String literal = literalOrUrl.first, url = literalOrUrl.second;
    if (literal != null) {
      if (literal.isEmpty()) {
        throw new AnalysisException("Avro schema literal is empty: " +
            tblName.toString());
      }
      // Validate that the literal is valid JSON and a valid Avro schema.
      try {
        List<ColumnDef> avroCols = AvroSchemaParser.parse(literal);
        if (colDefs != null) {
          colDefs.addAll(avroCols);
        }
      } catch (SchemaParseException e) {
        throw new AnalysisException(String.format(
            "Error parsing Avro schema for table '%s': %s", tblName,
            e.getMessage()));
      }
    } else if (url != null) {
      if (url.isEmpty()) {
        throw new AnalysisException("Avro schema url is empty: " + tblName.toString());
      }
      // Register a privilege request for the URL so authorization is checked.
      // Schema fetching, parsing, and column reconciliation are all deferred to
      // CatalogOpExecutor, which runs after authorization is evaluated.
      new HdfsUri(url).analyze(analyzer, Privilege.ALL, FsAction.READ);
      return true;
    }
    return false;
  }

  /**
   * Gets an Avro table's JSON schema from the list of given table property search
   * locations. The schema may be specified as a string literal or provided as a
   * Hadoop FileSystem or http(s) URL that points to the schema. Apart from ensuring
   * that the JSON schema is not SCHEMA_NONE, this function does not perform any
   * additional validation on the returned string (e.g., it may not be a valid
   * schema). Returns the Avro schema or null if none was specified in the search
   * locations. Throws an AnalysisException if a schema was specified, but could not
   * be retrieved, e.g., because of an invalid URL.
   */
  public static String getAvroSchema(List<Map<String, String>> avroSchemaLocs)
      throws AnalysisException {
    Pair<String, String> literalOrUrl = getAvroSchemaLiteralOrUrl(avroSchemaLocs);
    if (literalOrUrl.first != null) return literalOrUrl.first;
    if (literalOrUrl.second != null) return getSchemaFrom(literalOrUrl.second);
    return null;
  }

  /**
   * Gets an Avro table's JSON schema from a remote source provided as a URI. This method
   * can be called by DDLs after calling analyzeAvroSchema - which parses literals - and
   * after checking authorization.
   */
  public static String getRemoteAvroSchema(List<Map<String, String>> avroSchemaLocs)
      throws AnalysisException {
    Pair<String, String> literalOrUrl = getAvroSchemaLiteralOrUrl(avroSchemaLocs);
    if (literalOrUrl.second != null) return getSchemaFrom(literalOrUrl.second);
    return null;
  }

  private static String getSchemaFrom(String url) throws AnalysisException {
    if (BackendConfig.INSTANCE.disableCatalogDataOpsDebugOnly()) {
      LOG.info("Avro schema, {}, not loaded from fs: catalog data ops disabled.", url);
      return null;
    }
    try {
      Path path = new Path(url);
      String scheme = path.toUri().getScheme();
      if (scheme != null && Arrays.asList("http", "https").contains(scheme)) {
        if (!BackendConfig.INSTANCE.isAvroSchemaUrlRemoteHttpEnabled()) {
          throw new AnalysisException("avro.schema.url does not permit HTTP(S). Set " +
              "--avro_schema_url_remote_http_enabled=true to enable.");
        }
        String allowedHosts = BackendConfig.INSTANCE.getAvroSchemaUrlHttpAllowedHosts();
        if (allowedHosts.isEmpty()) {
          throw new AnalysisException("avro.schema.url HTTP(S) fetching requires " +
              "--avro_schema_url_http_allowed_hosts to be set.");
        }
        String host = new URL(url).getHost();
        if (host == null || host.isEmpty()) {
          throw new AnalysisException("avro.schema.url HTTP(S) fetching requires a " +
              "valid host in the URL.");
        }
        List<String> allowed = Arrays.asList(allowedHosts.toLowerCase().split(","));
        if (!allowed.contains(host.toLowerCase())) {
          throw new AnalysisException(String.format(
              "Host '%s' is not permitted for avro.schema.url HTTP(S) fetching. " +
              "Allowed hosts: %s", host, allowedHosts));
        }
        // HTTPS returns a HttpsURLConnection, which extends HttpURLConnection.
        HttpURLConnection conn =
            (HttpURLConnection) new URL(url).openConnection();
        // Disable redirect following to prevent SSRF: a redirect from an
        // allowlisted host to an internal host would otherwise bypass the
        // allowlist check above.
        conn.setInstanceFollowRedirects(false);
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(120_000);
        int responseCode = conn.getResponseCode();
        if (responseCode >= 300 && responseCode < 400) {
          throw new AnalysisException(String.format(
              "avro.schema.url HTTP(S) redirect is not permitted (host '%s' returned " +
              "status %d). Redirects are disabled to prevent server-side request " +
              "forgery.", host, responseCode));
        }
        try (InputStream in = conn.getInputStream()) {
          return IOUtils.toString(in, StandardCharsets.UTF_8);
        }
      } else {
        String allowedSchemes = BackendConfig.INSTANCE.getAvroSchemaUrlAllowedSchemes();
        List<String> allowed = Arrays.asList(allowedSchemes.toLowerCase().split(","));
        if (scheme != null && !allowed.contains(scheme.toLowerCase())) {
          throw new AnalysisException(String.format(
              "URI scheme '%s' is not permitted for avro.schema.url. " +
              "Allowed schemes: %s", scheme, allowedSchemes));
        }
        return FileSystemUtil.readFile(path);
      }
    } catch (IOException ioe) {
      throw new AnalysisException("Unable to read schema from given path: " + url, ioe);
    }
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
    List<ColumnDef> reconciledColDefs = reconcileSchemas(colDefs, avroCols, warning);
    if (warning.length() != 0) {
      LOG.warn(String.format("Warning while loading table %s.%s:\n%s",
          msTbl.getDbName(), msTbl.getTableName(), warning.toString()));
    }
    setFromSerdeComment(reconciledColDefs);
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

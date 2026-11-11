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

package org.apache.impala.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.catalogmanager.FeCatalogManager;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TIcebergDdlExecRequest;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.CatalogBlacklistUtils;
import org.apache.impala.util.IcebergUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Executes DDL on the coordinator through the Iceberg REST catalog that owns its target.
 *
 * <p>This is the DDL counterpart of {@link IcebergDmlFinalizer}. Frontend routes a DDL
 * here, instead of to CatalogD, when the target table belongs to a uniquely named REST
 * catalog, whether or not it could be loaded (see {@link IcebergRestDdlRouter}); the
 * backend then calls
 * back into the frontend with a TIcebergDdlExecRequest. CatalogD never learns about
 * these tables, so nothing is cached for them on the coordinator and the returned
 * TDdlExecResponse carries no catalog version to wait for.
 *
 * <p>Supported DDL: DROP TABLE, DROP VIEW IF EXISTS as its no-op counterpart, and
 * CREATE / DROP DATABASE (DROP without CASCADE). Other DDL types are rejected here,
 * and the analyzer refuses them for REST-backed tables before they get this far.
 */
final class IcebergDdlExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergDdlExecutor.class);

  private IcebergDdlExecutor() {}

  /** Executes the DDL in 'request' through its owning REST catalog. */
  static TDdlExecResponse execDdl(
      FeCatalogManager catalogManager, TIcebergDdlExecRequest request)
      throws ImpalaException {
    String catalogName = request.getRest_catalog_name();
    TDdlExecRequest ddl = request.getDdl_params();
    TDdlExecResponse response = createResponse();
    switch (ddl.getDdl_type()) {
      case DROP_TABLE:
      case DROP_VIEW:
        dropTable(catalogManager, catalogName, ddl.getDrop_table_or_view_params(),
            response);
        break;
      case DROP_DATABASE:
        dropDatabase(catalogManager, catalogName, ddl.getDrop_db_params(), response);
        break;
      case CREATE_DATABASE:
        createDatabase(catalogManager, catalogName, ddl.getCreate_db_params(), response);
        break;
      default:
        throw new ImpalaRuntimeException(String.format(
            "%s is not supported through Iceberg REST catalog %s",
            ddl.getDdl_type(), catalogName));
    }
    return response;
  }

  /**
   * Creates a response whose catalog update is empty: the coordinator caches nothing
   * for REST catalog tables, so there is no catalog version for the backend to wait for.
   */
  private static TDdlExecResponse createResponse() {
    TCatalogUpdateResult result = new TCatalogUpdateResult();
    result.setCatalog_service_id(new TUniqueId(0, 0));
    result.setVersion(0);
    result.setStatus(new TStatus(TErrorCode.OK, Collections.emptyList()));
    result.setIs_invalidate(false);
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(result);
    return response;
  }

  /**
   * Drops a table through its REST catalog. Nothing is loaded first: the REST server
   * removes the table, together with its data when PURGE was requested, and reports
   * whether it existed. This keeps DROP TABLE working for tables whose metadata can no
   * longer be read. REST catalogs serve tables only, so DROP VIEW never finds its
   * target here. Like CatalogD, this never drops a blacklisted table. The analyzer hides
   * it and answers IF EXISTS on it as a no-op, so this only guards against a request
   * that bypassed analysis.
   */
  private static void dropTable(FeCatalogManager catalogManager, String catalogName,
      TDropTableOrViewParams params, TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkNotNull(params, "Missing DROP TABLE parameters");
    // The names are as typed. Analysis and routing use lower case, as CatalogD does.
    String dbName = params.getTable_name().getDb_name().toLowerCase();
    String tblName = params.getTable_name().getTable_name().toLowerCase();
    if (CatalogBlacklistUtils.isDbBlacklisted(dbName)
        || CatalogBlacklistUtils.isTableBlacklisted(dbName, tblName)) {
      String message = String.format("Can't drop blacklisted table: %s.%s", dbName,
          tblName);
      if (!params.if_exists) throw new ImpalaRuntimeException(message);
      CatalogOpExecutor.addSummary(response, message);
      return;
    }
    if (params.isSetIs_table() && !params.is_table) {
      reportMissingTarget(params, dbName, tblName, response, "View");
      return;
    }
    boolean dropped;
    try {
      dropped = catalogManager.getIcebergCatalog(catalogName)
          .dropTable(dbName, tblName, params.purge);
    } catch (NoSuchNamespaceException e) {
      String message = "Database does not exist: " + dbName;
      if (!params.if_exists) throw new DatabaseNotFoundException(message);
      CatalogOpExecutor.addSummary(response, message);
      return;
    }
    if (!dropped) {
      reportMissingTarget(params, dbName, tblName, response, "Table");
      return;
    }
    LOG.info("Dropped table {}.{} through Iceberg REST catalog {}{}", dbName, tblName,
        catalogName, params.purge ? " with purge" : "");
    CatalogOpExecutor.addSummary(response, "Table has been dropped.");
  }

  /**
   * Reports that the table or view ('kind') 'dbName'.'tblName' to drop does not exist:
   * an error without IF EXISTS, a result row with IF EXISTS.
   */
  private static void reportMissingTarget(TDropTableOrViewParams params, String dbName,
      String tblName, TDdlExecResponse response, String kind)
      throws TableNotFoundException {
    if (!params.if_exists) {
      throw new TableNotFoundException(String.format("%s does not exist: %s.%s", kind,
          dbName, tblName));
    }
    CatalogOpExecutor.addSummary(response, kind + " does not exist.");
  }

  /**
   * Drops a database, i.e. a namespace, through its REST catalog. REST catalogs drop
   * empty namespaces only, so CASCADE is not supported, and a namespace that still holds
   * tables or views is rejected by the server. Like CatalogD, this never drops a
   * blacklisted database; the analyzer already hides it.
   */
  private static void dropDatabase(FeCatalogManager catalogManager, String catalogName,
      TDropDbParams params, TDdlExecResponse response) throws ImpalaException {
    Preconditions.checkNotNull(params, "Missing DROP DATABASE parameters");
    String dbName = params.getDb().toLowerCase();
    if (CatalogBlacklistUtils.isDbBlacklisted(dbName)) {
      String message = "Can't drop blacklisted database: " + dbName;
      if (!params.if_exists) throw new ImpalaRuntimeException(message);
      CatalogOpExecutor.addSummary(response, message);
      return;
    }
    if (params.cascade) {
      throw new ImpalaRuntimeException(
          "DROP DATABASE CASCADE is not supported on Iceberg REST catalogs: " + dbName);
    }
    boolean dropped;
    try {
      dropped = catalogManager.getIcebergCatalog(catalogName).dropNamespace(dbName);
    } catch (NamespaceNotEmptyException | AlreadyExistsException e) {
      throw new ImpalaRuntimeException("Cannot drop non-empty database: " + dbName, e);
    }
    if (!dropped) {
      if (!params.if_exists) {
        throw new DatabaseNotFoundException("Database does not exist: " + dbName);
      }
      CatalogOpExecutor.addSummary(response, "Database does not exist.");
      return;
    }
    LOG.info("Dropped database {} through Iceberg REST catalog {}", dbName, catalogName);
    CatalogOpExecutor.addSummary(response, "Database has been dropped.");
  }

  /**
   * Creates a database, i.e. a namespace, through its REST catalog. The comment, the
   * location and the database properties become namespace properties; the REST server
   * decides which it supports.
   */
  private static void createDatabase(FeCatalogManager catalogManager,
      String catalogName, TCreateDbParams params, TDdlExecResponse response)
      throws ImpalaException {
    Preconditions.checkNotNull(params, "Missing CREATE DATABASE parameters");
    String dbName = params.getDb().toLowerCase();
    if (CatalogBlacklistUtils.isDbBlacklisted(dbName)) {
      throw new ImpalaRuntimeException("Can't create blacklisted database: " + dbName);
    }
    String reserved = IcebergUtil.findReservedProperty(
        params.isSetProperties()
            ? params.getProperties().keySet() : Collections.emptySet(),
        IcebergUtil.REST_RESERVED_DB_PROPERTIES);
    if (reserved != null) {
      throw new ImpalaRuntimeException(reservedDbPropertyError(reserved, dbName));
    }
    Map<String, String> properties = new HashMap<>();
    if (params.isSetProperties()) properties.putAll(params.getProperties());
    if (params.isSetComment()) properties.put("comment", params.getComment());
    if (params.isSetLocation()) properties.put("location", params.getLocation());
    // TODO(IMPALA-15508): set the owner to params.getOwner(), as CatalogD does. The
    // HadoopCatalog-backed test server rejects namespace properties so far.
    try {
      catalogManager.getIcebergCatalog(catalogName).createNamespace(dbName, properties);
    } catch (AlreadyExistsException e) {
      if (!params.if_not_exists) {
        throw new ImpalaRuntimeException("Database already exists: " + dbName, e);
      }
      CatalogOpExecutor.addSummary(response, "Database already exists.");
      return;
    }
    LOG.info("Created database {} through Iceberg REST catalog {}", dbName, catalogName);
    CatalogOpExecutor.addSummary(response, "Database has been created.");
  }

  static String reservedDbPropertyError(String property, String dbName) {
    return String.format("Database property '%s' cannot be set on Iceberg REST " +
        "catalogs: %s", property, dbName);
  }
}

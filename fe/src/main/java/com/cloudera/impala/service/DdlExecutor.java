// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.ColumnNotFoundException;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.HiveStorageDescriptorFactory;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.catalog.PartitionNotFoundException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.TableNotFoundException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TAlterTableAddPartitionParams;
import com.cloudera.impala.thrift.TAlterTableAddReplaceColsParams;
import com.cloudera.impala.thrift.TAlterTableChangeColParams;
import com.cloudera.impala.thrift.TAlterTableDropColParams;
import com.cloudera.impala.thrift.TAlterTableDropPartitionParams;
import com.cloudera.impala.thrift.TAlterTableOrViewRenameParams;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetFileFormatParams;
import com.cloudera.impala.thrift.TAlterTableSetLocationParams;
import com.cloudera.impala.thrift.TAlterTableSetTblPropertiesParams;
import com.cloudera.impala.thrift.TCatalogUpdateResult;
import com.cloudera.impala.thrift.TColumnDef;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TCreateDbParams;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TCreateOrAlterViewParams;
import com.cloudera.impala.thrift.TCreateTableLikeParams;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TDdlExecResponse;
import com.cloudera.impala.thrift.TDropDbParams;
import com.cloudera.impala.thrift.TDropFunctionParams;
import com.cloudera.impala.thrift.TDropTableOrViewParams;
import com.cloudera.impala.thrift.TFileFormat;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.thrift.TUpdateMetastoreRequest;
import com.cloudera.impala.thrift.TUpdateMetastoreResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Class used to execute DDL operations.
 * TODO: Look in to ways to avoid the explosion of exception types in the throws
 * clause of these methods.
 */
public class DdlExecutor {
  private final CatalogServiceCatalog catalog;

  // Lock used to synchronize metastore CREATE/DROP/ALTER TABLE/DATABASE requests.
  private final Object metastoreDdlLock = new Object();
  private static final Logger LOG = Logger.getLogger(DdlExecutor.class);

  // Only applies to partition updates after an INSERT for now.
  private static final int NUM_CONCURRENT_METASTORE_OPERATIONS = 16;

  // Used to execute metastore updates in parallel. Currently only used for bulk
  // partition creations.
  private final ExecutorService executor =
      Executors.newFixedThreadPool(NUM_CONCURRENT_METASTORE_OPERATIONS);

  public DdlExecutor(CatalogServiceCatalog catalog) {
    this.catalog = catalog;
  }

  public TDdlExecResponse execDdlRequest(TDdlExecRequest ddlRequest)
      throws MetaException, NoSuchObjectException, InvalidOperationException, TException,
      TableLoadingException, ImpalaException {
    TDdlExecResponse response = new TDdlExecResponse();
    response.setResult(new TCatalogUpdateResult());
    response.getResult().setCatalog_service_id(JniCatalog.getServiceId());

    switch (ddlRequest.ddl_type) {
      case ALTER_TABLE:
        alterTable(ddlRequest.getAlter_table_params(), response);
        break;
      case ALTER_VIEW:
        alterView(ddlRequest.getAlter_view_params(), response);
        break;
      case CREATE_DATABASE:
        createDatabase(ddlRequest.getCreate_db_params(), response);
        break;
      case CREATE_TABLE_AS_SELECT:
        response.setNew_table_created(
            createTable(ddlRequest.getCreate_table_params(), response));
        break;
      case CREATE_TABLE:
        createTable(ddlRequest.getCreate_table_params(), response);
        break;
      case CREATE_TABLE_LIKE:
        createTableLike(ddlRequest.getCreate_table_like_params(), response);
        break;
      case CREATE_VIEW:
        createView(ddlRequest.getCreate_view_params(), response);
        break;
      case CREATE_FUNCTION:
        createFunction(ddlRequest.getCreate_fn_params(), response);
        break;
      case DROP_DATABASE:
        dropDatabase(ddlRequest.getDrop_db_params(), response);
        break;
      case DROP_TABLE:
      case DROP_VIEW:
        dropTableOrView(ddlRequest.getDrop_table_or_view_params(), response);
        break;
      case DROP_FUNCTION:
        dropFunction(ddlRequest.getDrop_fn_params(), response);
        break;
      default: throw new IllegalStateException("Unexpected DDL exec request type: " +
          ddlRequest.ddl_type);
    }
    // At this point, the operation is considered successful. If any errors occurred
    // during execution, this function will throw an exception and the CatalogServer
    // will handle setting a bad status code.
    response.getResult().setStatus(new TStatus(TStatusCode.OK, new ArrayList<String>()));
    return response;
  }

  /**
   * Execute the ALTER TABLE command according to the TAlterTableParams and refresh the
   * table metadata (except RENAME).
   */
  private void alterTable(TAlterTableParams params, TDdlExecResponse response)
      throws ImpalaException, MetaException, org.apache.thrift.TException,
      InvalidObjectException, ImpalaException, TableLoadingException {
    switch (params.getAlter_type()) {
      case ADD_REPLACE_COLUMNS:
        TAlterTableAddReplaceColsParams addReplaceColParams =
            params.getAdd_replace_cols_params();
        alterTableAddReplaceCols(TableName.fromThrift(params.getTable_name()),
            addReplaceColParams.getColumns(),
            addReplaceColParams.isReplace_existing_cols());
        break;
      case ADD_PARTITION:
        TAlterTableAddPartitionParams addPartParams = params.getAdd_partition_params();
        alterTableAddPartition(TableName.fromThrift(params.getTable_name()),
            addPartParams.getPartition_spec(), addPartParams.getLocation(),
            addPartParams.isIf_not_exists());
        break;
      case DROP_COLUMN:
        TAlterTableDropColParams dropColParams = params.getDrop_col_params();
        alterTableDropCol(TableName.fromThrift(params.getTable_name()),
            dropColParams.getCol_name());
        break;
      case CHANGE_COLUMN:
        TAlterTableChangeColParams changeColParams = params.getChange_col_params();
        alterTableChangeCol(TableName.fromThrift(params.getTable_name()),
            changeColParams.getCol_name(), changeColParams.getNew_col_def());
        break;
      case DROP_PARTITION:
        TAlterTableDropPartitionParams dropPartParams = params.getDrop_partition_params();
        alterTableDropPartition(TableName.fromThrift(params.getTable_name()),
            dropPartParams.getPartition_spec(), dropPartParams.isIf_exists());
        break;
      case RENAME_TABLE:
      case RENAME_VIEW:
        TAlterTableOrViewRenameParams renameParams = params.getRename_params();
        alterTableOrViewRename(TableName.fromThrift(params.getTable_name()),
            TableName.fromThrift(renameParams.getNew_table_name()),
            response);
        // Renamed table can't be fast refreshed anyway. Return now.
        return;
      case SET_FILE_FORMAT:
        TAlterTableSetFileFormatParams fileFormatParams =
            params.getSet_file_format_params();
        List<TPartitionKeyValue> fileFormatPartitionSpec = null;
        if (fileFormatParams.isSetPartition_spec()) {
          fileFormatPartitionSpec = fileFormatParams.getPartition_spec();
        }
        alterTableSetFileFormat(TableName.fromThrift(params.getTable_name()),
            fileFormatPartitionSpec, fileFormatParams.getFile_format());
        break;
      case SET_LOCATION:
        TAlterTableSetLocationParams setLocationParams = params.getSet_location_params();
        List<TPartitionKeyValue> partitionSpec = null;
        if (setLocationParams.isSetPartition_spec()) {
          partitionSpec = setLocationParams.getPartition_spec();
        }
        alterTableSetLocation(TableName.fromThrift(params.getTable_name()),
            partitionSpec, setLocationParams.getLocation());
        break;
      case SET_TBL_PROPERTIES:
        alterTableSetTblProperties(TableName.fromThrift(params.getTable_name()),
            params.getSet_tbl_properties_params());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown ALTER TABLE operation type: " + params.getAlter_type());
    }
    response.result.setVersion(catalog.resetTable(params.getTable_name(), true));
  }

  /**
   * Alters an existing view's definition in the metastore. Throws an exception
   * if the view does not exist or if the existing metadata entry is
   * a table instead of a a view.
   */
  private void alterView(TCreateOrAlterViewParams params, TDdlExecResponse resp)
      throws CatalogException, MetaException, TException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.alterView");

    synchronized (metastoreDdlLock) {
      // Operate on a copy of the metastore table to avoid prematurely applying the
      // alteration to our cached table in case the actual alteration fails.
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      if (!msTbl.getTableType().equalsIgnoreCase((TableType.VIRTUAL_VIEW.toString()))) {
        throw new InvalidObjectException(
            String.format("ALTER VIEW not allowed on a table: %s",
                tableName.toString()));
      }

      // Set the altered view attributes and update the metastore.
      setViewAttributes(params, msTbl);
      LOG.debug(String.format("Altering view %s", tableName));
      applyAlterTable(msTbl);
    }
    resp.result.setVersion(catalog.resetTable(tableName.toThrift(), true));
  }

  /**
   * Creates a new database in the metastore and adds the db name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the create, these
   * may vary depending on the Meta Store connection type (thrift vs direct db).
   *
   * @param dbName - The name of the new database.
   * @param comment - Comment to attach to the database, or null for no comment.
   * @param location - Hdfs path to use as the default location for new table data or
   *                   null to use default location.
   * @param ifNotExists - If true, no errors are thrown if the database already exists
   */
  private void createDatabase(TCreateDbParams params, TDdlExecResponse resp)
      throws MetaException, AlreadyExistsException, InvalidObjectException,
      org.apache.thrift.TException {
    Preconditions.checkNotNull(params);
    String dbName = params.getDb();
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    if (params.if_not_exists && catalog.getDb(dbName) != null) {
      LOG.debug("Skipping database creation because " + dbName + " already exists and " +
          "IF NOT EXISTS was specified.");
      resp.getResult().setVersion(Catalog.getCatalogVersion());
      return;
    }
    org.apache.hadoop.hive.metastore.api.Database db =
        new org.apache.hadoop.hive.metastore.api.Database();
    db.setName(dbName);
    if (params.getComment() != null) {
      db.setDescription(params.getComment());
    }
    if (params.getLocation() != null) {
      db.setLocationUri(params.getLocation());
    }
    LOG.debug("Creating database " + dbName);
    synchronized (metastoreDdlLock) {
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        msClient.getHiveClient().createDatabase(db);
      } catch (AlreadyExistsException e) {
        if (!params.if_not_exists) {
          throw e;
        }
        LOG.debug(String.format("Ignoring '%s' when creating database %s because " +
            "IF NOT EXISTS was specified.", e, dbName));
      } finally {
        msClient.release();
      }
    }
    resp.result.setVersion(catalog.addDb(dbName));
  }

  private void createFunction(TCreateFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException, MetaException, AlreadyExistsException {
    Function fn = Function.fromThrift(params.getFn());
    LOG.debug(String.format("Adding %s: %s",
        fn.getClass().getSimpleName(), fn.signatureString()));
    boolean added = catalog.addFunction(fn);
    if (!added && !params.if_not_exists) {
      throw new AlreadyExistsException("Function " + fn.signatureString() +
          " already exists.");
    }
    resp.result.setVersion(fn.getCatalogVersion());
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. The database must be empty (contain no tables) for the drop operation
   * to succeed. Re-throws any Hive Meta Store exceptions encountered during the drop.
   *
   * @param dbName - The name of the database to drop
   * @param ifExists - If true, no errors will be thrown if the database does not exist.
   */
  private void dropDatabase(TDropDbParams params, TDdlExecResponse resp)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    Preconditions.checkNotNull(params);
    Preconditions.checkState(params.getDb() != null && !params.getDb().isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");

    LOG.debug("Dropping database " + params.getDb());
    Db db = catalog.getDb(params.db);
    if (db != null && db.numFunctions() > 0) {
      throw new InvalidObjectException("Database " + db.getName() + " is not empty");
    }
    MetaStoreClient msClient = catalog.getMetaStoreClient();
    synchronized (metastoreDdlLock) {
      try {
        msClient.getHiveClient()
            .dropDatabase(params.getDb(), false, params.if_exists);
      } finally {
        msClient.release();
      }
    }
    resp.result.setVersion(catalog.removeDb(params.getDb()));
  }

  /**
   * Drop a table or view from the metastore and remove it from our cache.
   */
  private void dropTableOrView(TDropTableOrViewParams params, TDdlExecResponse resp)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    LOG.debug(String.format("Dropping table/view %s", tableName));
    synchronized (metastoreDdlLock) {
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        msClient.getHiveClient().dropTable(
            tableName.getDb(), tableName.getTbl(), true, params.if_exists);
      } finally {
        msClient.release();
      }
    }
    resp.result.setVersion(catalog.removeTable(params.getTable_name()));
  }

  private void dropFunction(TDropFunctionParams params, TDdlExecResponse resp)
      throws ImpalaException, MetaException, NoSuchObjectException {
    ArrayList<PrimitiveType> argTypes = Lists.newArrayList();
    for (TPrimitiveType t: params.arg_types) {
      argTypes.add(PrimitiveType.fromThrift(t));
    }
    Function desc = new Function(new FunctionName(params.fn_name),
        argTypes, PrimitiveType.INVALID_TYPE, false);
    LOG.debug(String.format("Dropping UDF %s", desc.signatureString()));
    long version = catalog.removeFunction(desc);
    if (version == Catalog.INITIAL_CATALOG_VERSION) {
      if (!params.if_exists) {
        throw new NoSuchObjectException(
            "Function: " + desc.signatureString() + " does not exist.");
      } else {
        // The user specified IF NOT EXISTS and the function didn't exist, just
        // return the current catalog version.
        version = Catalog.getCatalogVersion();
      }
    }
    resp.result.setVersion(version);
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Hive Meta Store
   * exceptions encountered during the create.
   *
   * @param tableName - Fully qualified name of the new table.
   * @param column - List of column definitions for the new table.
   * @param partitionColumn - List of partition column definitions for the new table.
   * @param owner - Owner of this table.
   * @param isExternal
   *    If true, table is created as external which means the data will not be deleted
   *    if dropped. External tables can also be created on top of existing data.
   * @param comment - Optional comment to attach to the table (null for no comment).
   * @param location - Hdfs path to use as the location for table data or null to use
   *                   default location.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   * @return Returns true if a new table was created in the metastore as a result of this
   *         call. Returns false if creation was skipped - this indicates the table already
   *         existed and the caller specified IF NOT EXISTS.
   */
  private boolean createTable(TCreateTableParams params, TDdlExecResponse response)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException,
      TableLoadingException {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
        "Null or empty column list given as argument to Catalog.createTable");

    if (params.if_not_exists &&
        catalog.containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.debug(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tableName));
      response.getResult().setVersion(Catalog.getCatalogVersion());
      return false;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl =
        createMetaStoreTable(params);
    LOG.debug(String.format("Creating table %s", tableName));
    return createTable(tbl, params.if_not_exists, response);
  }

  /**
   * Creates a new view in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Metastore
   * exceptions encountered during the create.
   */
  private void createView(TCreateOrAlterViewParams params, TDdlExecResponse response)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException, TableLoadingException {
    TableName tableName = TableName.fromThrift(params.getView_name());
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    Preconditions.checkState(params.getColumns() != null &&
        params.getColumns().size() > 0,
          "Null or empty column list given as argument to DdlExecutor.createView");
    if (params.if_not_exists &&
        catalog.containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.debug(String.format("Skipping view creation because %s already exists and " +
          "ifNotExists is true.", tableName));
    }

    // Create new view.
    org.apache.hadoop.hive.metastore.api.Table view =
        new org.apache.hadoop.hive.metastore.api.Table();
    setViewAttributes(params, view);
    LOG.debug(String.format("Creating view %s", tableName));
    createTable(view, params.if_not_exists, response);
  }

  /**
   * Creates a new table in the metastore based on the definition of an existing table.
   * No data is copied as part of this process, it is a metadata only operation. If the
   * creation succeeds, an entry is added to the metadata cache to lazily load the new
   * table's metadata on the next access.
   *
   * @param tableName - Fully qualified name of the new table.
   * @param srcTableName - Fully qualified name of the old table.
   * @param owner - Owner of this table.
   * @param isExternal
   *    If true, table is created as external which means the data will not be deleted
   *    if dropped. External tables can also be created on top of existing data.
   * @param comment - Optional comment to attach to the table or an empty string for no
                      comment. Null to copy comment from the source table.
   * @param fileFormat - The file format for the new table or null to copy file format
   *                     from source table.
   * @param location - Hdfs path to use as the location for table data or null to use
   *                   default location.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  private void createTableLike(TCreateTableLikeParams params, TDdlExecResponse response)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException,
      ImpalaException, TableLoadingException, TableNotFoundException {
    Preconditions.checkNotNull(params);

    TFileFormat fileFormat = params.isSetFile_format() ? params.getFile_format() : null;
    String comment = params.isSetComment() ? params.getComment() : null;
    TableName tblName = TableName.fromThrift(params.getTable_name());
    TableName srcTblName = TableName.fromThrift(params.getSrc_table_name());
    Preconditions.checkState(tblName != null && tblName.isFullyQualified());
    Preconditions.checkState(srcTblName != null && srcTblName.isFullyQualified());

    if (params.if_not_exists &&
        catalog.containsTable(tblName.getDb(), tblName.getTbl())) {
      LOG.debug(String.format("Skipping table creation because %s already exists and " +
          "IF NOT EXISTS was specified.", tblName));
      response.getResult().setVersion(Catalog.getCatalogVersion());
      return;
    }
    Table srcTable = catalog.getTable(srcTblName.getDb(), srcTblName.getTbl());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    tbl.setDbName(tblName.getDb());
    tbl.setTableName(tblName.getTbl());
    tbl.setOwner(params.getOwner());
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    // The EXTERNAL table property should not be copied from the old table.
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      if (tbl.getParameters().containsKey("EXTERNAL")) {
        tbl.getParameters().remove("EXTERNAL");
      }
    }
    // The LOCATION property should not be copied from the old table. If the location
    // is null (the caller didn't specify a custom location) this will clear the value
    // and the table will use the default table location from the parent database.
    tbl.getSd().setLocation(params.getLocation());
    if (fileFormat != null) {
      setStorageDescriptorFileFormat(tbl.getSd(), fileFormat);
    }
    LOG.debug(String.format("Creating table %s LIKE %s", tblName, srcTblName));
    createTable(tbl, params.if_not_exists, response);
  }

  private boolean createTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      boolean ifNotExists, TDdlExecResponse response) throws MetaException,
      NoSuchObjectException, AlreadyExistsException, InvalidObjectException,
      org.apache.thrift.TException, TableLoadingException {
    MetaStoreClient msClient = catalog.getMetaStoreClient();
    synchronized (metastoreDdlLock) {
      try {
        msClient.getHiveClient().createTable(newTable);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.debug(String.format("Ignoring '%s' when creating table %s.%s because " +
            "IF NOT EXISTS was specified.", e,
            newTable.getDbName(), newTable.getTableName()));
        return false;
      } finally {
        msClient.release();
      }
    }

    response.result.setVersion(catalog.addTable(
        newTable.getDbName(), newTable.getTableName()));
    return true;
  }

  /**
   * Sets the given params in the metastore table as appropriate for a view.
   */
  private void setViewAttributes(TCreateOrAlterViewParams params,
      org.apache.hadoop.hive.metastore.api.Table view) {
    view.setTableType(TableType.VIRTUAL_VIEW.toString());
    view.setViewOriginalText(params.getOriginal_view_def());
    view.setViewExpandedText(params.getExpanded_view_def());
    view.setDbName(params.getView_name().getDb_name());
    view.setTableName(params.getView_name().getTable_name());
    view.setOwner(params.getOwner());
    if (view.getParameters() == null) view.setParameters(new HashMap<String, String>());
    if (params.isSetComment() &&  params.getComment() != null) {
      view.getParameters().put("comment", params.getComment());
    }

    // Add all the columns to a new storage descriptor.
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    // Set a dummy SerdeInfo for Hive.
    sd.setSerdeInfo(new SerDeInfo());
    view.setSd(sd);
  }

  /**
   * Appends one or more columns to the given table, optionally replacing all existing
   * columns.
   */
  private void alterTableAddReplaceCols(TableName tableName, List<TColumnDef> columns,
      boolean replaceExistingCols) throws MetaException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

    List<FieldSchema> newColumns = buildFieldSchemaList(columns);
    if (replaceExistingCols) {
      msTbl.getSd().setCols(newColumns);
    } else {
      // Append the new column to the existing list of columns.
      for (FieldSchema fs: buildFieldSchemaList(columns)) {
        msTbl.getSd().addToCols(fs);
      }
    }
    applyAlterTable(msTbl);
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  private void alterTableChangeCol(TableName tableName, String colName,
      TColumnDef newColDef) throws MetaException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
       TableLoadingException, ColumnNotFoundException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      // Find the matching column name and change it.
      Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
      while (iterator.hasNext()) {
        FieldSchema fs = iterator.next();
        if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
          TColumnDesc colDesc = newColDef.getColumnDesc();
          fs.setName(colDesc.getColumnName());
          fs.setType(colDesc.getColumnType().toString().toLowerCase());
          // Don't overwrite the existing comment unless a new comment is given
          if (newColDef.getComment() != null) {
            fs.setComment(newColDef.getComment());
          }
          break;
        }
        if (!iterator.hasNext()) {
          throw new ColumnNotFoundException(String.format(
              "Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Adds a new partition to the given table.
   */
  private void alterTableAddPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location, boolean ifNotExists)
      throws MetaException, AlreadyExistsException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    if (ifNotExists && catalog.containsHdfsPartition(tableName.getDb(),
        tableName.getTbl(), partitionSpec)) {
      LOG.debug(String.format("Skipping partition creation because (%s) already exists " +
          "and ifNotExists is true.", Joiner.on(", ").join(partitionSpec)));
      return;
    }

    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      partition.setDbName(tableName.getDb());
      partition.setTableName(tableName.getTbl());

      List<String> values = Lists.newArrayList();
      // Need to add in the values in the same order they are defined in the table.
      for (FieldSchema fs: msTbl.getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            values.add(kv.getValue());
          }
        }
      }
      partition.setValues(values);
      StorageDescriptor sd = msTbl.getSd().deepCopy();
      sd.setLocation(location);
      partition.setSd(sd);
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        msClient.getHiveClient().add_partition(partition);
        updateLastDdlTime(msTbl, msClient);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.debug(String.format("Ignoring '%s' when adding partition to %s because" +
            " ifNotExists is true.", e, tableName));
      } finally {
        msClient.release();
      }
    }
  }

  /**
   * Drops an existing partition from the given table.
   */
  private void alterTableDropPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, boolean ifExists) throws MetaException,
      NoSuchObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {

    if (ifExists && !catalog.containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
        partitionSpec)) {
      LOG.debug(String.format("Skipping partition drop because (%s) does not exist " +
          "and ifExists is true.", Joiner.on(", ").join(partitionSpec)));
      return;
    }

    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      List<String> values = Lists.newArrayList();
      // Need to add in the values in the same order they are defined in the table.
      for (FieldSchema fs: msTbl.getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            values.add(kv.getValue());
          }
        }
      }
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        msClient.getHiveClient().dropPartition(tableName.getDb(),
            tableName.getTbl(), values);
        updateLastDdlTime(msTbl, msClient);
      } catch (NoSuchObjectException e) {
        if (!ifExists) {
          throw e;
        }
        LOG.debug(String.format("Ignoring '%s' when dropping partition from %s because" +
            " ifExists is true.", e, tableName));
      } finally {
        msClient.release();
      }
    }
  }

  /**
   * Removes a column from the given table.
   */
  private void alterTableDropCol(TableName tableName, String colName)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException,
      DatabaseNotFoundException, TableNotFoundException, ColumnNotFoundException,
      TableLoadingException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

      // Find the matching column name and remove it.
      Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
      while (iterator.hasNext()) {
        FieldSchema fs = iterator.next();
        if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
          iterator.remove();
          break;
        }
        if (!iterator.hasNext()) {
          throw new ColumnNotFoundException(String.format(
              "Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Renames an existing table or view. After renaming the table/view,
   * its metadata is marked as invalid and will be reloaded on the next access.
   */
  private void alterTableOrViewRename(TableName tableName, TableName newTableName,
      TDdlExecResponse response)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException,
      DatabaseNotFoundException, TableNotFoundException, TableLoadingException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      msTbl.setDbName(newTableName.getDb());
      msTbl.setTableName(newTableName.getTbl());
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        msClient.getHiveClient().alter_table(
            tableName.getDb(), tableName.getTbl(), msTbl);
      } finally {
        msClient.release();
      }
    }
    // Rename the table in the Catalog and get the version.
    response.result.setVersion(
        catalog.renameTable(tableName.toThrift(), newTableName.toThrift()));
  }

  /**
   * Changes the file format for the given table or partition. This is a metadata only
   * operation, existing table data will not be converted to the new format. After
   * changing the file format the table metadata is marked as invalid and will be
   * reloaded on the next access.
   */
  private void alterTableSetFileFormat(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, TFileFormat fileFormat) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock) {
        HdfsPartition partition = catalog.getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), partitionSpec);
        org.apache.hadoop.hive.metastore.api.Partition msPartition =
            partition.getMetaStorePartition();
        Preconditions.checkNotNull(msPartition);
        setStorageDescriptorFileFormat(msPartition.getSd(), fileFormat);
        try {
          applyAlterPartition(tableName, msPartition);
        } finally {
          partition.markDirty();
        }
      }
    }
  }

  /**
   * Helper method for setting the file format on a given storage descriptor.
   */
  private static void setStorageDescriptorFileFormat(StorageDescriptor sd,
      TFileFormat fileFormat) {
    StorageDescriptor tempSd =
        HiveStorageDescriptorFactory.createSd(fileFormat, RowFormat.DEFAULT_ROW_FORMAT);
    sd.setInputFormat(tempSd.getInputFormat());
    sd.setOutputFormat(tempSd.getOutputFormat());
    sd.getSerdeInfo().setSerializationLib(tempSd.getSerdeInfo().getSerializationLib());
  }

  /**
   * Changes the HDFS storage location for the given table. This is a metadata only
   * operation, existing table data will not be as part of changing the location.
   */
  private void alterTableSetLocation(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        msTbl.getSd().setLocation(location);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock) {
        HdfsPartition partition = catalog.getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), partitionSpec);
        org.apache.hadoop.hive.metastore.api.Partition msPartition =
            partition.getMetaStorePartition();
        Preconditions.checkNotNull(msPartition);
        msPartition.getSd().setLocation(location);
        try {
          applyAlterPartition(tableName, msPartition);
        } finally {
          partition.markDirty();
        }
      }
    }
  }

  /**
   * Appends to the table property metadata for the given table, replacing the values
   * of any keys that already exist.
   */
  private void alterTableSetTblProperties(TableName tableName,
      TAlterTableSetTblPropertiesParams params) throws MetaException,
      InvalidObjectException, TException, DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {
    Map<String, String> properties = params.getProperties();
    Preconditions.checkNotNull(properties);
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      switch (params.getTarget()) {
        case TBL_PROPERTY:
          msTbl.getParameters().putAll(properties);
          break;
        case SERDE_PROPERTY:
          msTbl.getSd().getSerdeInfo().getParameters().putAll(properties);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown target TTablePropertyType: " + params.getTarget());
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Applies an ALTER TABLE command to the metastore table. The caller should take the
   * metastoreDdlLock before calling this method.
   * Note: The metastore interface is not very safe because it only accepts a
   * an entire metastore.api.Table object rather than a delta of what to change. This
   * means an external modification to the table could be overwritten by an ALTER TABLE
   * command if the metadata is not completely in-sync. This affects both Hive and
   * Impala, but is more important in Impala because the metadata is cached for a
   * longer period of time.
   */
  private void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException {
    MetaStoreClient msClient = catalog.getMetaStoreClient();
    long lastDdlTime = -1;
    try {
      lastDdlTime = calculateDdlTime(msTbl);
      msTbl.putToParameters("transient_lastDdlTime", Long.toString(lastDdlTime));
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    } finally {
      msClient.release();
      catalog.updateLastDdlTime(
          new TTableName(msTbl.getDbName(), msTbl.getTableName()), lastDdlTime);
    }
  }

  private void applyAlterPartition(TableName tableName,
      org.apache.hadoop.hive.metastore.api.Partition msPartition) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {
    MetaStoreClient msClient = catalog.getMetaStoreClient();
    try {
      msClient.getHiveClient().alter_partition(
          tableName.getDb(), tableName.getTbl(), msPartition);
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      updateLastDdlTime(msTbl, msClient);
    } finally {
      msClient.release();
    }
  }

  /**
   * Returns a deep copy of the metastore.api.Table object for the given TableName.
   */
  private org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable(
      TableName tableName) throws DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException {
    Preconditions.checkState(tableName != null && tableName.isFullyQualified());
    return catalog.getTable(tableName.getDb(), tableName.getTbl())
        .getMetaStoreTable().deepCopy();
  }

  public static List<FieldSchema> buildFieldSchemaList(List<TColumnDef> columnDefs) {
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    for (TColumnDef c: columnDefs) {
      TColumnDesc colDesc = c.getColumnDesc();
      FieldSchema fs = new FieldSchema(colDesc.getColumnName(),
          colDesc.getColumnType().toString().toLowerCase(), c.getComment());
      fsList.add(fs);
    }
    return fsList;
  }

   /**
   * Sets the table parameter 'transient_lastDdlTime' to System.currentTimeMillis()/1000
   * in the given msTbl. 'transient_lastDdlTime' is guaranteed to be changed.
   * If msClient is not null then this method applies alter_table() to update the
   * Metastore. Otherwise, the caller is responsible for the final update.
   */
  public long updateLastDdlTime(org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaStoreClient msClient) throws MetaException, NoSuchObjectException, TException {
    Preconditions.checkNotNull(msTbl);
    LOG.debug("Updating lastDdlTime for table: " + msTbl.getTableName());
    Map<String, String> params = msTbl.getParameters();
    long lastDdlTime = calculateDdlTime(msTbl);
    params.put("transient_lastDdlTime", Long.toString(lastDdlTime));
    msTbl.setParameters(params);
    if (msClient != null) {
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    }
    catalog.updateLastDdlTime(
        new TTableName(msTbl.getDbName(), msTbl.getTableName()), lastDdlTime);
    return lastDdlTime;
  }

  /**
   * Calculates the next transient_lastDdlTime value. This is exactly how Hive updates
   * the last ddl time.
   */
  private static long calculateDdlTime(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // This is exactly how Hive updates the last ddl time.
    long existingLastDdlTime = Catalog.getLastDdlTime(msTbl);
    long currentTime = System.currentTimeMillis() / 1000;
    while (existingLastDdlTime == currentTime) {
      // We need to make sure that lastDdlTime will be changed but we don't want to set
      // lastDdlTime to a future time when this function returns. If the last DDL and
      // this one happened within a sec, sleep for a second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      currentTime = System.currentTimeMillis() / 1000;
    }
    return currentTime;
  }

  /**
   * Utility function that creates a hive.metastore.api.Table object based on the given
   * TCreateTableParams.
   * TODO: Extract metastore object creation utility functions into a separate
   * helper/factory class.
   */
  public static org.apache.hadoop.hive.metastore.api.Table
      createMetaStoreTable(TCreateTableParams params) {
    Preconditions.checkNotNull(params);
    TableName tableName = TableName.fromThrift(params.getTable_name());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(tableName.getDb());
    tbl.setTableName(tableName.getTbl());
    tbl.setOwner(params.getOwner());
    if (params.isSetTable_properties()) {
      tbl.setParameters(params.getTable_properties());
    } else {
      tbl.setParameters(new HashMap<String, String>());
    }

    if (params.getComment() != null) {
      tbl.getParameters().put("comment", params.getComment());
    }
    if (params.is_external) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }

    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(
        params.getFile_format(), RowFormat.fromThrift(params.getRow_format()));

    if (params.isSetSerde_properties()) {
      if (sd.getSerdeInfo().getParameters() == null) {
        sd.getSerdeInfo().setParameters(params.getSerde_properties());
      } else {
        sd.getSerdeInfo().getParameters().putAll(params.getSerde_properties());
      }
    }

    if (params.getLocation() != null) {
      sd.setLocation(params.getLocation());
    }
    // Add in all the columns
    sd.setCols(buildFieldSchemaList(params.getColumns()));
    tbl.setSd(sd);
    if (params.getPartition_columns() != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(params.getPartition_columns()));
    } else {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    return tbl;
  }

  /**
   * Creates a single partition in the metastore.
   * TODO: Depending how often we do lots of metastore operations at once, might be worth
   * making this reusable.
   */
  private class CreatePartitionRunnable implements Runnable {
    /**
     * Constructs a new operation to create a partition in dbName.tblName called
     * partName. The supplied future is signalled if an error occurs, or if numPartitions
     * is decremented to 0 after the partition creation has completed. If a partition is
     * actually created, partitionCreated is set.
     */
    public CreatePartitionRunnable(TableName tblName,
        String partName, AtomicBoolean partitionCreated,
        SettableFuture<Void> allFinished, AtomicInteger numPartitions) {
      tblName_ = tblName;
      partName_ = partName;
      partitionCreated_ = partitionCreated;
      allFinished_ = allFinished;
      numPartitions_ = numPartitions;
    }

    public void run() {
      // If there was an exception in another operation, abort
      if (allFinished_.isDone()) return;
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        LOG.debug("Creating partition: " + partName_ + " in table: " + tblName_);
        msClient.getHiveClient().appendPartitionByName(tblName_.getDb(),
            tblName_.getTbl(), partName_);
        partitionCreated_.set(true);
      } catch (AlreadyExistsException e) {
        LOG.debug("Ignoring partition " + partName_ + ", since it already exists");
        // Ignore since partition already exists.
      } catch (Exception e) {
        allFinished_.setException(e);
      } finally {
        msClient.release();
      }

      // If this is the last operation to complete, signal the future
      if (numPartitions_.decrementAndGet() == 0) {
        allFinished_.set(null);
      }
    }

    private final TableName tblName_;
    private final String partName_;
    private final AtomicBoolean partitionCreated_;
    private final AtomicInteger numPartitions_;
    private final SettableFuture<Void> allFinished_;
  }

  /**
   * Create any new partitions required as a result of an INSERT statement.
   * Updates the lastDdlTime of the table if new partitions were created.
   */
  public TUpdateMetastoreResponse updateMetastore(TUpdateMetastoreRequest update)
      throws ImpalaException {
    TUpdateMetastoreResponse response = new TUpdateMetastoreResponse();
    // Only update metastore for Hdfs tables.
    Table table = catalog.getTable(update.getDb_name(), update.getTarget_table());
    if (!(table instanceof HdfsTable)) {
      throw new InternalException("Unexpected table type: " +
          update.getTarget_table());
    }

    TableName tblName = new TableName(table.getDb().getName(), table.getName());
    AtomicBoolean addedNewPartition = new AtomicBoolean(false);

    if (table.getNumClusteringCols() > 0) {
      SettableFuture<Void> allFinished = SettableFuture.create();
      AtomicInteger numPartitions =
          new AtomicInteger(update.getCreated_partitions().size());
      // Add all partitions to metastore.
      for (String partName: update.getCreated_partitions()) {
        Preconditions.checkState(partName != null && !partName.isEmpty());
        CreatePartitionRunnable rbl =
            new CreatePartitionRunnable(tblName, partName, addedNewPartition, allFinished,
                numPartitions);
        executor.execute(rbl);
      }

      try {
        // Will throw if any operation calls setException
        allFinished.get();
      } catch (Exception e) {
        throw new InternalException("Error updating metastore", e);
      }
    }
    if (addedNewPartition.get()) {
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      try {
        // Operate on a copy of msTbl to prevent our cached msTbl becoming inconsistent
        // if the alteration fails in the metastore.
        org.apache.hadoop.hive.metastore.api.Table msTbl =
            table.getMetaStoreTable().deepCopy();
        updateLastDdlTime(msTbl, msClient);
      } catch (Exception e) {
        throw new InternalException("Error updating lastDdlTime", e);
      } finally {
        msClient.release();
      }
    }
    response.setResult(new TCatalogUpdateResult(JniCatalog.getServiceId(),
        catalog.resetTable(tblName.toThrift(), true),
        new TStatus(TStatusCode.OK, new ArrayList<String>())));
    return response;
  }
}

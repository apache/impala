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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.TransactionException;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.HiveMetadataFormatUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

/**
 * Base class for Hive 3 MetastoreShim.
 */
public class Hive3MetastoreShimBase {
  private static final Logger LOG = LoggerFactory.getLogger(Hive3MetastoreShimBase.class);

  protected static final String EXTWRITE = "EXTWRITE";
  protected static final String EXTREAD = "EXTREAD";
  protected static final String HIVEBUCKET2 = "HIVEBUCKET2";
  protected static final String HIVEFULLACIDREAD = "HIVEFULLACIDREAD";
  protected static final String HIVEFULLACIDWRITE = "HIVEFULLACIDWRITE";
  protected static final String HIVEMANAGEDINSERTREAD = "HIVEMANAGEDINSERTREAD";
  protected static final String HIVEMANAGEDINSERTWRITE = "HIVEMANAGEDINSERTWRITE";
  protected static final String HIVEMANAGESTATS = "HIVEMANAGESTATS";
  // Materialized View
  protected static final String HIVEMQT = "HIVEMQT";
  // Virtual View
  protected static final String HIVESQL = "HIVESQL";
  protected static final long MAJOR_VERSION = 3;
  protected static boolean capabilitiestSet_ = false;

  // Max sleep interval during acquiring an ACID lock.
  private static final long MAX_SLEEP_INTERVAL_MS = 30000;

  protected final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";

  // Id used to register transactions / locks.
  // Not final, as it makes sense to set it based on role + instance, see IMPALA-8853.
  public static String TRANSACTION_USER_ID = "Impala";

  /**
   * Initializes and returns a TblTransaction object for table 'tbl'. Opens a new
   * transaction if txnId is not valid.
   */
  public static TblTransaction createTblTransaction(
      IMetaStoreClient client, Table tbl, long txnId)
      throws TransactionException {
    TblTransaction tblTxn = new TblTransaction();
    try {
      if (txnId <= 0) {
        txnId = openTransaction(client);
        tblTxn.ownsTxn = true;
      }
      tblTxn.txnId = txnId;
      tblTxn.writeId =
          allocateTableWriteId(client, txnId, tbl.getDbName(), tbl.getTableName());
      tblTxn.validWriteIds =
          getValidWriteIdListInTxn(client, tbl.getDbName(), tbl.getTableName(), txnId);
      return tblTxn;
    } catch (TException e) {
      if (tblTxn.ownsTxn) {
        abortTransactionNoThrow(client, tblTxn.txnId);
      }
      throw new TransactionException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTblTransaction"), e);
    }
  }

  static public void commitTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) throws TransactionException {
    if (tblTxn.ownsTxn) {
      commitTransaction(client, tblTxn.txnId);
    }
  }

  static public void abortTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) {
    if (tblTxn.ownsTxn) {
      abortTransactionNoThrow(client, tblTxn.txnId);
    }
  }

  /**
   * Constant variable that stores engine value needed to store / access Impala column
   * statistics.
   */
  public static final String IMPALA_ENGINE = "impala";

  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.

  public static boolean validateName(String name) {
    return MetaStoreServerUtils.validateName(name, null);
  }
*/

  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(
        partition.getDbName(), partition.getTableName(), partition, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions, null);
  }

  /**
   * Wrapper around IMetaStoreClient.createTableWithConstraints() to deal with added
   * arguments. Hive four new arguments are uniqueConstraints, notNullConstraints,
   * defaultConstraints, and checkConstraints.
   */
  public static void createTableWithConstraints(IMetaStoreClient client,
      Table newTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
      throws InvalidOperationException, MetaException, TException {
    client.createTableWithConstraints(newTbl, primaryKeys, foreignKeys, null, null,
        null, null);
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, org.apache.hadoop.hive.metastore.api.Table tbl,
      Warehouse warehouse) throws MetaException {
    MetaStoreServerUtils.updatePartitionStatsFast(partition, tbl, warehouse, /*madeDir*/false,
        /*forceRecompute*/false,
        /*environmentContext*/null, /*isCreate*/false);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return MetastoreConf.ConfVars.BATCH_RETRIEVE_OBJECTS_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to mark that
   * the stats were generated automatically by a stats task.
   */
  public static Pair<String, String> statsGeneratedViaStatsTaskParam() {
    return Pair.create(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
  }

  public static TResultSet execGetFunctions(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetFunctionsReq req = request.getGet_functions_req();
    return MetadataOp.getFunctions(
        frontend, req.getCatalogName(), req.getSchemaName(), req.getFunctionName(), user);
  }

  public static TResultSet execGetColumns(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetColumnsReq req = request.getGet_columns_req();
    return MetadataOp.getColumns(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getColumnName(), user);
  }

  public static TResultSet execGetTables(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetTablesReq req = request.getGet_tables_req();
    return MetadataOp.getTables(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getTableTypes(), user);
  }

  public static TResultSet execGetSchemas(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetSchemasReq req = request.getGet_schemas_req();
    return MetadataOp.getSchemas(
        frontend, req.getCatalogName(), req.getSchemaName(), user);
  }

  /**
   * Supported HMS-3 types
   */
  public static final EnumSet<TableType> IMPALA_SUPPORTED_TABLE_TYPES = EnumSet
      .of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW,
          TableType.MATERIALIZED_VIEW);

  /**
   * mapping between the HMS-3 type the Impala types
   */
  public static final ImmutableMap<String, TImpalaTableType>
      HMS_TO_IMPALA_TYPE = new ImmutableMap.Builder<String, TImpalaTableType>()
          .put("EXTERNAL_TABLE", TImpalaTableType.TABLE)
          .put("MANAGED_TABLE", TImpalaTableType.TABLE)
          .put("INDEX_TABLE", TImpalaTableType.TABLE)
          .put("VIRTUAL_VIEW", TImpalaTableType.VIEW)
          .put("MATERIALIZED_VIEW", TImpalaTableType.MATERIALIZED_VIEW).build();

  /**
   * Method which maps Metastore's TableType to Impala's table type. In metastore 2
   * Materialized view is not supported
   */
  public static TImpalaTableType mapToInternalTableType(String typeStr) {
    TImpalaTableType defaultTableType = TImpalaTableType.TABLE;
    TableType tType;

    if (typeStr == null) return defaultTableType;
    try {
      tType = TableType.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      return defaultTableType;
    }
    switch (tType) {
      case EXTERNAL_TABLE:
      case MANAGED_TABLE:
        return TImpalaTableType.TABLE;
      case VIRTUAL_VIEW:
        return TImpalaTableType.VIEW;
      case MATERIALIZED_VIEW:
        return TImpalaTableType.MATERIALIZED_VIEW;
      default:
        return defaultTableType;
    }
  }

  // hive-3 introduces a catalog object in hive
  // Impala only supports the default catalog of hive
  private static final String DEFAULT_CATALOG_NAME = MetaStoreUtils
      .getDefaultCatalog(MetastoreConf.newMetastoreConf());

  /**
   * Gets the name of the default catalog from metastore configuration.
   */
  public static String getDefaultCatalogName() {
    return DEFAULT_CATALOG_NAME;
  }

  /**
   * Returns whether the catalog name is the default catalog.
   */
  public static boolean isDefaultCatalog(String catalogName) {
    return DEFAULT_CATALOG_NAME.equalsIgnoreCase(catalogName);
  }

  /**
   * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3. This
   * method uses the metastore's FileUtils method instead of one from hive-exec
   *
   * @param partitionColNames
   * @param values
   * @return
   */
  public static String makePartName(List<String> partitionColNames, List<String> values) {
    return FileUtils.makePartName(partitionColNames, values);
  }

  /**
   * Wrapper method to get the formatted string to represent the columns information of a
   * metastore table. This method was changed in Hive-3 significantly when compared to
   * Hive-2. In order to avoid adding unnecessary dependency to hive-exec this method
   * copies the source code from hive-2's MetaDataFormatUtils class for this method.
   * TODO : In order to avoid this copy, we move move this code from hive's ql module
   * to a util method in MetastoreUtils in metastore module
   *
   * @return
   */
  public static String getAllColumnsInformation(List<FieldSchema> tabCols,
      List<FieldSchema> partitionCols, boolean printHeader, boolean isOutputPadded,
      boolean showPartColsSeparately) {
    return HiveMetadataFormatUtils
        .getAllColumnsInformation(tabCols, partitionCols, printHeader, isOutputPadded,
            showPartColsSeparately);
  }

  /**
   * Converts a List of FieldSchema to String
   */
  public static String getPartitionTransformInformation(
      List<FieldSchema> partitionTransformCols) {
    return HiveMetadataFormatUtils.getPartitionTransformInformation(
        partitionTransformCols);
  }

  /**
   * Wrapper method around Hive's MetadataFormatUtils.getTableInformation which has
   * changed significantly in Hive-3
   *
   * @return
   */
  public static String getTableInformation(Table table) {
    return HiveMetadataFormatUtils.getTableInformation(table, false);
  }

  /**
   * Wrapper method around Hive-3's MetadataFormatUtils.getConstraintsInformation
   *
   * @return
   */
  public static String getConstraintsInformation(PrimaryKeyInfo pkInfo,
      ForeignKeyInfo fkInfo) {
    return HiveMetadataFormatUtils.getConstraintsInformation(pkInfo, fkInfo);
  }

  /**
   * This method has been copied from BaseSemanticAnalyzer class of Hive and is fairly
   * stable now (last change was in mid 2016 as of April 2019). Copying is preferred over
   * adding dependency to this class which pulls in a lot of other transitive dependencies
   * from hive-exec
   */
  public static String unescapeSQLString(String stringLiteral) {
    {
      Character enclosure = null;

      // Some of the strings can be passed in as unicode. For example, the
      // delimiter can be passed in as \002 - So, we first check if the
      // string is a unicode number, else go back to the old behavior
      StringBuilder sb = new StringBuilder(stringLiteral.length());
      for (int i = 0; i < stringLiteral.length(); i++) {

        char currentChar = stringLiteral.charAt(i);
        if (enclosure == null) {
          if (currentChar == '\'' || stringLiteral.charAt(i) == '\"') {
            enclosure = currentChar;
          }
          // ignore all other chars outside the enclosure
          continue;
        }

        if (enclosure.equals(currentChar)) {
          enclosure = null;
          continue;
        }

        if (currentChar == '\\' && (i + 6 < stringLiteral.length())
            && stringLiteral.charAt(i + 1) == 'u') {
          int code = 0;
          int base = i + 2;
          for (int j = 0; j < 4; j++) {
            int digit = Character.digit(stringLiteral.charAt(j + base), 16);
            code = (code << 4) + digit;
          }
          sb.append((char) code);
          i += 5;
          continue;
        }

        if (currentChar == '\\' && (i + 4 < stringLiteral.length())) {
          char i1 = stringLiteral.charAt(i + 1);
          char i2 = stringLiteral.charAt(i + 2);
          char i3 = stringLiteral.charAt(i + 3);
          if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
              && (i3 >= '0' && i3 <= '7')) {
            byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
            byte[] bValArr = new byte[1];
            bValArr[0] = bVal;
            String tmp = new String(bValArr);
            sb.append(tmp);
            i += 3;
            continue;
          }
        }

        if (currentChar == '\\' && (i + 2 < stringLiteral.length())) {
          char n = stringLiteral.charAt(i + 1);
          switch (n) {
            case '0':
              sb.append("\0");
              break;
            case '\'':
              sb.append("'");
              break;
            case '"':
              sb.append("\"");
              break;
            case 'b':
              sb.append("\b");
              break;
            case 'n':
              sb.append("\n");
              break;
            case 'r':
              sb.append("\r");
              break;
            case 't':
              sb.append("\t");
              break;
            case 'Z':
              sb.append("\u001A");
              break;
            case '\\':
              sb.append("\\");
              break;
            // The following 2 lines are exactly what MySQL does TODO: why do we do this?
            case '%':
              sb.append("\\%");
              break;
            case '_':
              sb.append("\\_");
              break;
            default:
              sb.append(n);
          }
          i++;
        } else {
          sb.append(currentChar);
        }
      }
      return sb.toString();
    }
  }

  /**
   * Get ValidWriteIdList object by given string
   *
   * @param validWriteIds ValidWriteIdList object in String
   * @return ValidWriteIdList object
   */
  public static ValidWriteIdList getValidWriteIdListFromString(String validWriteIds) {
    Preconditions.checkNotNull(validWriteIds);
    return new ValidReaderWriteIdList(validWriteIds);
  }

  /**
   * Converts a TValidWriteIdList object to ValidWriteIdList.
   *
   * @param tableName     the name of the table.
   * @param validWriteIds the thrift object.
   * @return ValidWriteIdList object
   */
  public static ValidWriteIdList getValidWriteIdListFromThrift(String tableName,
      TValidWriteIdList validWriteIds) {
    Preconditions.checkNotNull(validWriteIds);
    BitSet abortedBits;
    if (validWriteIds.getAborted_indexesSize() > 0) {
      abortedBits = new BitSet(validWriteIds.getInvalid_write_idsSize());
      for (int aborted_index : validWriteIds.getAborted_indexes()) {
        abortedBits.set(aborted_index);
      }
    } else {
      abortedBits = new BitSet();
    }
    long highWatermark = validWriteIds.isSetHigh_watermark() ?
        validWriteIds.high_watermark : Long.MAX_VALUE;
    long minOpenWriteId = validWriteIds.isSetMin_open_write_id() ?
        validWriteIds.min_open_write_id : Long.MAX_VALUE;
    return new ValidReaderWriteIdList(tableName,
        validWriteIds.getInvalid_write_ids().stream().mapToLong(i -> i).toArray(),
        abortedBits, highWatermark, minOpenWriteId);
  }

  /**
   * Converts a ValidWriteIdList object to TValidWriteIdList.
   */
  public static TValidWriteIdList convertToTValidWriteIdList(
      ValidWriteIdList validWriteIdList) {
    Preconditions.checkNotNull(validWriteIdList);
    TValidWriteIdList ret = new TValidWriteIdList();
    long minOpenWriteId = validWriteIdList.getMinOpenWriteId() != null ?
        validWriteIdList.getMinOpenWriteId() : Long.MAX_VALUE;
    ret.setHigh_watermark(validWriteIdList.getHighWatermark());
    ret.setMin_open_write_id(minOpenWriteId);
    ret.setInvalid_write_ids(Arrays.stream(
        validWriteIdList.getInvalidWriteIds()).boxed().collect(Collectors.toList()));
    List<Integer> abortedIndexes = new ArrayList<>();
    for (int i = 0; i < validWriteIdList.getInvalidWriteIds().length; ++i) {
      long writeId = validWriteIdList.getInvalidWriteIds()[i];
      if (validWriteIdList.isWriteIdAborted(writeId)) {
        abortedIndexes.add(i);
      }
    }
    ret.setAborted_indexes(abortedIndexes);
    return ret;
  }

  /**
   * Returns a ValidTxnList object that helps to identify in-progress and aborted
   * transactions.
   */
  public static ValidTxnList getValidTxns(IMetaStoreClient client) throws TException {
    return client.getValidTxns();
  }

  /**
   * Get validWriteIds in string with txnId and table name arguments.
   */
  private static String getValidWriteIdListInTxn(IMetaStoreClient client, String dbName,
      String tblName, long txnId)
      throws TException {
    ValidTxnList txns = client.getValidTxns(txnId);
    String tableFullName = dbName + "." + tblName;
    List<TableValidWriteIds> writeIdsObj = client.getValidWriteIds(
        Lists.newArrayList(tableFullName), txns.toString());
    ValidTxnWriteIdList validTxnWriteIdList = new ValidTxnWriteIdList(txnId);
    for (TableValidWriteIds tableWriteIds : writeIdsObj) {
      validTxnWriteIdList.addTableValidWriteIdList(
          createValidReaderWriteIdList(tableWriteIds));
    }
    String validWriteIds =
        validTxnWriteIdList.getTableValidWriteIdList(tableFullName).writeToString();
    return validWriteIds;
  }

  /**
   * Opens a new transaction. Sets userId to TRANSACTION_USER_ID.
   *
   * @param client is the HMS client to be used.
   * @return the new transaction id.
   * @throws TransactionException
   */
  public static long openTransaction(IMetaStoreClient client)
      throws TransactionException {
    try {
      return client.openTxn(TRANSACTION_USER_ID);
    } catch (Exception e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * Commits a transaction.
   *
   * @param client is the HMS client to be used.
   * @param txnId  is the transaction id.
   * @throws TransactionException
   */
  public static void commitTransaction(IMetaStoreClient client, long txnId)
      throws TransactionException {
    try {
      client.commitTxn(txnId);
    } catch (Exception e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * Aborts a transaction.
   *
   * @param client is the HMS client to be used.
   * @param txnId  is the transaction id.
   * @throws TransactionException
   */
  public static void abortTransaction(IMetaStoreClient client, long txnId)
      throws TransactionException {
    try {
      client.abortTxns(Arrays.asList(txnId));
    } catch (Exception e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * Heartbeats a transaction and/or lock to keep them alive.
   *
   * @param client is the HMS client to be used.
   * @param txnId  is the transaction id.
   * @param lockId is the lock id.
   * @return True on success, false if the transaction or lock is non-existent anymore.
   * @throws TransactionException In case of any other failures.
   */
  public static boolean heartbeat(IMetaStoreClient client,
      long txnId, long lockId) throws TransactionException {
    String errorMsg = "Caught exception during heartbeating transaction " +
        String.valueOf(txnId) + " lock " + String.valueOf(lockId);
    LOG.info("Sending heartbeat for transaction " + String.valueOf(txnId) +
        " lock " + String.valueOf(lockId));
    try {
      client.heartbeat(txnId, lockId);
    } catch (NoSuchLockException e) {
      LOG.info(errorMsg, e);
      return false;
    } catch (NoSuchTxnException e) {
      LOG.info(errorMsg, e);
      return false;
    } catch (TxnAbortedException e) {
      LOG.info(errorMsg, e);
      return false;
    } catch (TException e) {
      throw new TransactionException(e.getMessage());
    }
    return true;
  }

  /**
   * Creates a lock for the given lock components. Returns the acquired lock, this might
   * involve some waiting.
   *
   * @param client               is the HMS client to be used.
   * @param txnId                The transaction ID associated with the lock. Zero if the
   *                             lock doesn't belong to a transaction.
   * @param lockComponents       the lock components to include in this lock.
   * @param maxWaitTimeInSeconds Maximum wait time to acquire the lock.
   * @return the lock id
   * @throws TransactionException in case of failure
   */
  public static long acquireLock(IMetaStoreClient client, long txnId,
      List<LockComponent> lockComponents, int maxWaitTimeInSeconds)
      throws TransactionException {
    LockRequestBuilder lockRequestBuilder = new LockRequestBuilder();
    lockRequestBuilder.setUser(TRANSACTION_USER_ID);
    if (txnId > 0) {
      lockRequestBuilder.setTransactionId(txnId);
    }
    for (LockComponent lockComponent : lockComponents) {
      lockRequestBuilder.addLockComponent(lockComponent);
    }
    LockRequest lockRequest = lockRequestBuilder.build();
    try {
      long startTime = System.currentTimeMillis();
      long timeoutTime = startTime + maxWaitTimeInSeconds * 1000;
      long sleepIntervalMs = 100;
      LockResponse lockResponse = client.lock(lockRequest);
      long lockId = lockResponse.getLockid();
      while (lockResponse.getState() == LockState.WAITING &&
             System.currentTimeMillis() < timeoutTime) {
        try {
          //TODO: add profile counter for lock waits.
          // Sleep 'sleepIntervalMs', or the amount of time left until 'timeoutTime'.
          long sleepMs = Math.min(sleepIntervalMs,
                                  Math.abs(timeoutTime - System.currentTimeMillis()));
          LOG.debug("Waiting " + String.valueOf(sleepMs) +
              " milliseconds for lock " + String.valueOf(lockId) + " of transaction " +
              Long.toString(txnId));
          Thread.sleep(sleepMs);
          sleepIntervalMs = Math.min(MAX_SLEEP_INTERVAL_MS,
                                     sleepIntervalMs * 2);
          lockResponse = client.checkLock(lockId);
        } catch (InterruptedException e) {
          // Since wait time is configurable it wouldn't add much value to make
          // acquireLock() interruptible so we just swallow the exception here.
        }
      }
      LockState lockState = lockResponse.getState();
      LOG.info("It took " + String.valueOf(System.currentTimeMillis() - startTime) +
          " ms to wait for lock " + String.valueOf(lockId) + " of transaction " +
          String.valueOf(txnId) + ". Final lock state is " + lockState.name());
      if (lockState == LockState.ACQUIRED) {
        return lockId;
      }
      if (lockId > 0) {
        try {
          releaseLock(client, lockId);
        } catch (TransactionException te) {
          LOG.error("Failed to release lock as a cleanup step after acquiring a lock " +
              "has failed: " + lockId + " " + te.getMessage());
        }
      }
      throw new TransactionException("Failed to acquire lock for transaction " +
          String.valueOf(txnId));
    } catch (TException e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * Releases a lock in HMS.
   *
   * @param client is the HMS client to be used.
   * @param lockId is the lock ID to be released.
   * @throws TransactionException
   */
  public static void releaseLock(IMetaStoreClient client, long lockId)
      throws TransactionException {
    try {
      client.unlock(lockId);
    } catch (Exception e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * Aborts a transaction and logs the error if there is an exception.
   *
   * @param client is the HMS client to be used.
   * @param txnId  is the transaction id.
   */
  public static void abortTransactionNoThrow(IMetaStoreClient client, long txnId) {
    try {
      client.abortTxns(Arrays.asList(txnId));
    } catch (Exception e) {
      LOG.error("Error in abortTxns.", e);
    }
  }

  /**
   * Allocates a write id for the given table.
   *
   * @param client    is the HMS client to be used.
   * @param txnId     is the transaction id.
   * @param dbName    is the database name.
   * @param tableName is the target table name.
   * @return the allocated write id.
   * @throws TransactionException
   */
  public static long allocateTableWriteId(IMetaStoreClient client, long txnId,
      String dbName, String tableName) throws TransactionException {
    try {
      return client.allocateTableWriteId(txnId, dbName, tableName);
    } catch (Exception e) {
      throw new TransactionException(e.getMessage());
    }
  }

  /**
   * @return the hive major version
   */
  public static long getMajorVersion() {
    return MAJOR_VERSION;
  }

  /**
   * Borrowed code from hive. This assumes that the caller intends to read the files, and
   * thus treats both open and aborted write ids as invalid.
   *
   * @param tableWriteIds valid write ids for the given table from the metastore
   * @return a valid write IDs list for the input table
   */
  private static ValidReaderWriteIdList createValidReaderWriteIdList(
      TableValidWriteIds tableWriteIds) {
    String fullTableName = tableWriteIds.getFullTableName();
    long highWater = tableWriteIds.getWriteIdHighWaterMark();
    List<Long> invalids = tableWriteIds.getInvalidWriteIds();
    BitSet abortedBits = BitSet.valueOf(tableWriteIds.getAbortedBits());
    long[] exceptions = new long[invalids.size()];
    int i = 0;
    for (long writeId : invalids) {
      exceptions[i++] = writeId;
    }
    if (tableWriteIds.isSetMinOpenWriteId()) {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits,
          highWater, tableWriteIds.getMinOpenWriteId());
    } else {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits,
          highWater);
    }
  }

  /**
   * Return the default table path for a new table.
   * <p>
   * Hive-3 doesn't allow managed table to be non transactional after HIVE-22158. Creating
   * a non transactional managed table will finally result in an external table with table
   * property "external.table.purge" set to true. As the table type become EXTERNAL, the
   * location will be under "metastore.warehouse.external.dir" (HIVE-19837, introduces in
   * hive-2.7, not in hive-2.1.x-cdh6.x yet).
   */
  public static String getPathForNewTable(Database db, Table tbl)
      throws MetaException {
    Warehouse wh = new Warehouse(new HiveConf());
    // Non transactional tables are all translated to external tables by HMS's default
    // transformer (HIVE-22158). Note that external tables can't be transactional.
    // So the request and result of the default transformer is:
    //     non transactional managed table => external table
    //     non transactional external table => external table
    //     transactional managed table => managed table
    //     transactional external table (not allowed)
    boolean isExternal = !AcidUtils.isTransactionalTable(tbl.getParameters());
    // TODO(IMPALA-9088): deal with customized transformer in HMS.
    return wh.getDefaultTablePath(db, tbl.getTableName().toLowerCase(), isExternal)
        .toString();
  }
}

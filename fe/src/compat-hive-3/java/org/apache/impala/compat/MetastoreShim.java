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

package org.apache.impala.compat;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_WRITEONLY;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_TABLE;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.TransactionException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for Hive 3.
 */
public class MetastoreShim {
  private static final Logger LOG = Logger.getLogger(MetastoreShim.class);

  private static final String EXTWRITE = "EXTWRITE";
  private static final String EXTREAD = "EXTREAD";
  private static final String HIVEBUCKET2 = "HIVEBUCKET2";
  private static final String HIVEFULLACIDREAD = "HIVEFULLACIDREAD";
  private static final String HIVEFULLACIDWRITE = "HIVEFULLACIDWRITE";
  private static final String HIVEMANAGEDINSERTREAD = "HIVEMANAGEDINSERTREAD";
  private static final String HIVEMANAGEDINSERTWRITE = "HIVEMANAGEDINSERTWRITE";
  private static final String HIVEMANAGESTATS = "HIVEMANAGESTATS";
  // Materialized View
  private static final String HIVEMQT = "HIVEMQT";
  // Virtual View
  private static final String HIVESQL = "HIVESQL";
  private static final long MAJOR_VERSION = 3;
  private static boolean capabilitiestSet_ = false;

  // Number of retries to acquire an HMS ACID lock.
  private static final int LOCK_RETRIES = 10;

  // Time interval between retries of acquiring an HMS ACID lock
  private static final int LOCK_RETRY_WAIT_SECONDS = 3;

  private final static String HMS_RPC_ERROR_FORMAT_STR =
      "Error making '%s' RPC to Hive Metastore: ";

  // Id used to register transactions / locks.
  // Not final, as it makes sense to set it based on role + instance, see IMPALA-8853.
  public static String TRANSACTION_USER_ID = "Impala";

  /**
   * Initializes and returns a TblTransaction object for table 'tbl'.
   * Opens a new transaction if txnId is not valid.
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
    }
    catch (TException e) {
      if (tblTxn.ownsTxn) abortTransactionNoThrow(client, tblTxn.txnId);
      throw new TransactionException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "createTblTransaction"), e);
    }
  }

  static public void commitTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) throws TransactionException {
    if (tblTxn.ownsTxn) commitTransaction(client, tblTxn.txnId);
  }

  static public void abortTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) {
    if (tblTxn.ownsTxn) abortTransactionNoThrow(client, tblTxn.txnId);
  }

  /**
   * Constant variable that stores engine value needed to store / access
   * Impala column statistics.
   */
  protected static final String IMPALA_ENGINE = "impala";

  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.
   */
  public static boolean validateName(String name) {
    return MetaStoreUtils.validateName(name, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_table with validWriteIds as a param.
   */
  public static void alterTableWithTransaction(IMetaStoreClient client,
     Table tbl, TblTransaction tblTxn)
     throws ImpalaRuntimeException {
    tbl.setWriteId(tblTxn.writeId);
    try {
      client.alter_table(null, tbl.getDbName(), tbl.getTableName(),
        tbl, null, tblTxn.validWriteIds);
    }
    catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
    }
  }


  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added
   * arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(
        partition.getDbName(), partition.getTableName(), partition, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added
   * arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions with transaction information
   */
  public static void alterPartitionsWithTransaction(IMetaStoreClient client,
      String dbName, String tblName, List<Partition> partitions, TblTransaction tblTxn
      ) throws InvalidOperationException, MetaException, TException {
    for (Partition part : partitions) {
      part.setWriteId(tblTxn.writeId);
    }
    // Correct validWriteIdList is needed
    // to commit the alter partitions operation in hms side.
    client.alter_partitions(dbName, tblName, partitions, null,
        tblTxn.validWriteIds, tblTxn.writeId);
  }

  /**
   * Wrapper around IMetaStoreClient.getTableColumnStatistics() to deal with added
   * arguments.
   */
  public static List<ColumnStatisticsObj> getTableColumnStatistics(
      IMetaStoreClient client, String dbName, String tableName, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException {
    return client.getTableColumnStatisticsV2(dbName, tableName, colNames,
        /*engine*/IMPALA_ENGINE);
  }

  /**
   * Wrapper around IMetaStoreClient.deleteTableColumnStatistics() to deal with added
   * arguments.
   */
  public static boolean deleteTableColumnStatistics(IMetaStoreClient client,
      String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
             InvalidInputException {
    return client.deleteTableColumnStatisticsV2(dbName, tableName, colName,
        /*engine*/IMPALA_ENGINE);
  }

  /**
   * Wrapper around ColumnStatistics c'tor to deal with the added engine property.
   */
  public static ColumnStatistics createNewHiveColStats() {
    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setEngine(IMPALA_ENGINE);
    return colStats;
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, Table tbl,
      Warehouse warehouse) throws MetaException {
    MetaStoreUtils.updatePartitionStatsFast(partition, tbl, warehouse, /*madeDir*/false,
        /*forceRecompute*/false,
        /*environmentContext*/null, /*isCreate*/false);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in
   * a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return MetastoreConf.ConfVars.BATCH_RETRIEVE_OBJECTS_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to
   * mark that the stats were generated automatically by a stats task.
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
  public static final ImmutableMap<String, String> HMS_TO_IMPALA_TYPE =
      new ImmutableMap.Builder<String, String>()
          .put("EXTERNAL_TABLE", TABLE_TYPE_TABLE)
          .put("MANAGED_TABLE", TABLE_TYPE_TABLE)
          .put("INDEX_TABLE", TABLE_TYPE_TABLE)
          .put("VIRTUAL_VIEW", TABLE_TYPE_VIEW)
          .put("MATERIALIZED_VIEW", TABLE_TYPE_VIEW).build();
  /**
   * Method which maps Metastore's TableType to Impala's table type. In metastore 2
   * Materialized view is not supported
   */
  public static String mapToInternalTableType(String typeStr) {
    String defaultTableType = TABLE_TYPE_TABLE;
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
      //Deprecated and removed in Hive-3.. //TODO throw exception?
      case INDEX_TABLE:
        return TABLE_TYPE_TABLE;
      case VIRTUAL_VIEW:
      case MATERIALIZED_VIEW:
        return TABLE_TYPE_VIEW;
      default:
        return defaultTableType;
    }
  }

  //hive-3 has a different class to encode and decode event messages
  private static final MessageEncoder eventMessageEncoder_ =
      MessageFactory.getDefaultInstance(MetastoreConf.newMetastoreConf());

  /**
   * Wrapper method which returns HMS-3 Message factory in case Impala is
   * building against Hive-3
   */
  public static MessageDeserializer getMessageDeserializer() {
    return eventMessageEncoder_.getDeserializer();
  }

  /**
   * Wrapper around HMS-3 message encoder to get the serializer
   * @return
   */
  public static MessageSerializer getMessageSerializer() {
    return eventMessageEncoder_.getSerializer();
  }

  /**
   * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3.
   * This method uses the metastore's FileUtils method instead of one from hive-exec
   * @param partitionColNames
   * @param values
   * @return
   */
  public static String makePartName(List<String> partitionColNames, List<String> values) {
    return FileUtils.makePartName(partitionColNames, values);
  }

  /**
   * Wrapper method around message factory's build alter table message due to added
   * arguments in hive 3.
   */
  @VisibleForTesting
  public static AlterTableMessage buildAlterTableMessage(Table before, Table after,
      boolean isTruncateOp, long writeId) {
    return MessageBuilder.getInstance().buildAlterTableMessage(before, after,
        isTruncateOp, writeId);
  }

  /**
   * Wrapper around HMS-3 message serializer
   * @param message
   * @return serialized string to use used in the NotificationEvent's message field
   */
  @VisibleForTesting
  public static String serializeEventMessage(EventMessage message) {
    return getMessageSerializer().serialize(message);
  }

  /**
   * Wrapper method to get the formatted string to represent the columns information of
   * a metastore table. This method was changed in Hive-3 significantly when compared
   * to Hive-2. In order to avoid adding unnecessary dependency to hive-exec this
   * method copies the source code from hive-2's MetaDataFormatUtils class for this
   * method.
   * TODO : In order to avoid this copy, we move move this code from hive's ql module
   * to a util method in MetastoreUtils in metastore module
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
   * Wrapper method around Hive's MetadataFormatUtils.getTableInformation which has
   * changed significantly in Hive-3
   * @return
   */
  public static String getTableInformation(
      org.apache.hadoop.hive.ql.metadata.Table table) {
    return HiveMetadataFormatUtils.getTableInformation(table.getTTable(), false);
  }

  /**
   * This method has been copied from BaseSemanticAnalyzer class of Hive and is fairly
   * stable now (last change was in mid 2016 as of April 2019). Copying is preferred over
   * adding dependency to this class which pulls in a lot of other transitive
   * dependencies from hive-exec
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

        if (currentChar == '\\' && (i + 6 < stringLiteral.length()) && stringLiteral.charAt(i + 1) == 'u') {
          int code = 0;
          int base = i + 2;
          for (int j = 0; j < 4; j++) {
            int digit = Character.digit(stringLiteral.charAt(j + base), 16);
            code = (code << 4) + digit;
          }
          sb.append((char)code);
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
   * Get valid write ids from HMS for the acid table
   * @param client the client to access HMS
   * @param tableFullName the name for the table
   * @return ValidWriteIdList object
   */
  public static ValidWriteIdList fetchValidWriteIds(IMetaStoreClient client,
      String tableFullName) throws TException {
    return client.getValidWriteIds(tableFullName);
  }

  /**
   * Get ValidWriteIdList object by given string
   * @param validWriteIds ValidWriteIdList object in String
   * @return ValidWriteIdList object
   */
  public static ValidWriteIdList getValidWriteIdListFromString(String validWriteIds) {
    Preconditions.checkNotNull(validWriteIds);
    return new ValidReaderWriteIdList(validWriteIds);
  }


  /**
   * Get validWriteIds in string with txnId and table name
   * arguments.
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
   * Wrapper around HMS Partition object to get writeID
   * WriteID is introduced in ACID 2
   * It is used to detect changes of the partition
   */
  public static long getWriteIdFromMSPartition(Partition partition) {
    Preconditions.checkNotNull(partition);
    return partition.getWriteId();
  }

  /**
   * Wrapper around HMS Table object to get writeID
   * Per table writeId is introduced in ACID 2
   * It is used to detect changes of the table
   */
  public static long getWriteIdFromMSTable(Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    return msTbl.getWriteId();
  }

  /**
   * Opens a new transaction.
   * Sets userId to TRANSACTION_USER_ID.
   * @param client is the HMS client to be used.
   * @param userId of user who is opening this transaction.
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
   * @param client is the HMS client to be used.
   * @param txnId is the transaction id.
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
   * @param client is the HMS client to be used.
   * @param txnId is the transaction id.
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
   * @param client is the HMS client to be used.
   * @param txnId is the transaction id.
   * @param lockId is the lock id.
   * @return True on success, false if the transaction or lock is non-existent
   * anymore.
   * @throws In case of any other failures.
   */
  public static boolean heartbeat(IMetaStoreClient client,
      long txnId, long lockId) throws TransactionException {
    String errorMsg = "Caught exception during heartbeating transaction " +
        String.valueOf(txnId) + " lock " + String.valueOf(lockId);
    LOG.info("Sending heartbeat");
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
   * Creates a lock for the given lock components. Returns the acquired lock, this
   * might involve some waiting.
   * @param client is the HMS client to be used.
   * @param txnId The transaction ID associated with the lock. Zero if the lock doesn't
   * belong to a transaction.
   * @param lockComponents the lock components to include in this lock.
   * @return the lock id
   * @throws TransactionException in case of failure
   */
  public static long acquireLock(IMetaStoreClient client, long txnId,
      List<LockComponent> lockComponents)
          throws TransactionException {
    LockRequestBuilder lockRequestBuilder = new LockRequestBuilder();
    lockRequestBuilder.setUser(TRANSACTION_USER_ID);
    if (txnId > 0) lockRequestBuilder.setTransactionId(txnId);
    for (LockComponent lockComponent : lockComponents) {
      lockRequestBuilder.addLockComponent(lockComponent);
    }
    LockRequest lockRequest = lockRequestBuilder.build();
    try {
      LockResponse lockResponse = client.lock(lockRequest);
      long lockId = lockResponse.getLockid();
      int retries = 0;
      while (lockResponse.getState() == LockState.WAITING && retries < LOCK_RETRIES) {
        try {
          Thread.sleep(LOCK_RETRY_WAIT_SECONDS * 1000);
          ++retries;
          lockResponse = client.checkLock(lockId);
        } catch (InterruptedException e) {
          // Since wait time and number of retries is configurable it wouldn't add
          // much value to make acquireLock() interruptible so we just swallow the
          // exception here.
        }
      }
      if (lockResponse.getState() == LockState.ACQUIRED) return lockId;
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
   * @param client is the HMS client to be used.
   * @param txnId is the transaction id.
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
   * @param client is the HMS client to be used.
   * @param txnId is the transaction id.
   * @param dbName is the database name.
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
   * Set impala capabilities to hive client
   * Impala supports:
   * - external table read/write
   * - insert-only Acid table read
   * - virtual view read
   * - materialized view read
   */
  public static synchronized void setHiveClientCapabilities() {
    String hostName;
    if (capabilitiestSet_) return;
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ue) {
      hostName = "unknown";
    }
    String buildVersion = BackendConfig.INSTANCE != null ?
        BackendConfig.INSTANCE.getImpalaBuildVersion() : String.valueOf(MAJOR_VERSION);
    if (buildVersion == null) buildVersion = String.valueOf(MAJOR_VERSION);

    String impalaId = String.format("Impala%s@%s", buildVersion, hostName);
    String[] capabilities = new String[] {
        EXTWRITE, // External table write
        EXTREAD,  // External table read
        HIVEMANAGEDINSERTREAD, // Insert-only table read
        HIVEMANAGEDINSERTWRITE, // Insert-only table write
        HIVESQL,
        HIVEMQT,
        HIVEBUCKET2 // Includes the capability to get the correct bucket number.
                    // Currently, without this capability, for an external bucketed
                    // table, Hive will return the table as Read-only with bucket
                    // number -1. It makes clients unable to know it is a bucketed table.
                    // TODO: will remove this capability when Hive can provide
                    // API calls to tell the changing of bucket number.
        };

    HiveMetaStoreClient.setProcessorIdentifier(impalaId);
    HiveMetaStoreClient.setProcessorCapabilities(capabilities);
    capabilitiestSet_ = true;
  }

  /**
   * Check if a table has a capability
   * @param msTble hms table
   * @param requireCapability hive access types or combination of them
   * @return true if the table has the capability
   */
  public static boolean hasTableCapability(Table msTbl, byte requiredCapability) {
    Preconditions.checkNotNull(msTbl);
    // access types in binary:
    // ACCESSTYPE_NONE:      00000001
    // ACCESSTYPE_READONLY:  00000010
    // ACCESSTYPE_WRITEONLY: 00000100
    // ACCESSTYPE_READWRITE: 00001000
    return requiredCapability != ACCESSTYPE_NONE
        && ((msTbl.getAccessType() & requiredCapability) != 0);
  }

  /**
   * Get Access type in string
   * @param msTble hms table
   * @return the string represents the table access type.
   */
  public static String getTableAccessType(Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    switch (msTbl.getAccessType()) {
      case ACCESSTYPE_READONLY:
        return "READONLY";
      case ACCESSTYPE_WRITEONLY:
        return "WRITEONLY";
      case ACCESSTYPE_READWRITE:
        return "READWRITE";
      case ACCESSTYPE_NONE:
      default:
        return "NONE";
    }
  }

  /**
   * Set table access type. This is useful for hms Table object constructed for create
   * table statement. For example, to create a table, we need Read/Write capabilities
   * not default 0(not defined)
   */
  public static void setTableAccessType(Table msTbl, byte accessType) {
    Preconditions.checkNotNull(msTbl);
    msTbl.setAccessType(accessType);
  }

  public static void setTableColumnStatsTransactional(IMetaStoreClient client,
      Table msTbl, ColumnStatistics colStats, TblTransaction tblTxn)
      throws ImpalaRuntimeException {
    List<ColumnStatistics> colStatsList = new ArrayList<>();
    colStatsList.add(colStats);
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStatsList);
    request.setWriteId(tblTxn.writeId);
    request.setValidWriteIdList(tblTxn.validWriteIds);
    try {
      // Despite its name, the function below can and (and currently must) be used
      // to set table level column statistics in transactional tables.
      client.setPartitionColumnStatistics(request);
    }
    catch (TException e) {
      throw new ImpalaRuntimeException(
          String.format(HMS_RPC_ERROR_FORMAT_STR, "setPartitionColumnStatistics"), e);
    }
  }

  /**
   * @return the hive major version
   */
  public static long getMajorVersion() {
    return MAJOR_VERSION;
  }

    /**
     * Borrowed code from hive.
     * This assumes that the caller intends to
     * read the files, and thus treats both open and aborted write ids as invalid.
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
   *
   * Hive-3 doesn't allow managed table to be non transactional after HIVE-22158.
   * Creating a non transactional managed table will finally result in an external table
   * with table property "external.table.purge" set to true. As the table type become
   * EXTERNAL, the location will be under "metastore.warehouse.external.dir" (HIVE-19837,
   * introduces in hive-2.7, not in hive-2.1.x-cdh6.x yet).
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

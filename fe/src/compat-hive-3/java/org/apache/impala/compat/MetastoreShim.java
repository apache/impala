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

import static org.apache.impala.service.MetadataOp.TABLE_TYPE_TABLE;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.HiveMetadataFormatUtils;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for Hive 3.
 */
public class MetastoreShim {
  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.
   */
  public static boolean validateName(String name) {
    return MetaStoreUtils.validateName(name, null);
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

  @VisibleForTesting
  public static InsertMessage buildInsertMessage(Table msTbl, Partition partition,
      boolean isInsertOverwrite, List<String> newFiles) {
    return MessageBuilder.getInstance().buildInsertMessage(msTbl, partition,
        isInsertOverwrite, newFiles.iterator());
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
   * @return the shim major version
   */
  public static long getMajorVersion() {
    return 3;
  }
}

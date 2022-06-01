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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadata;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ObjectDictionary;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.metastore.CatalogMetastoreServiceHandler;
import org.apache.impala.catalog.metastore.HmsApiNameEnum;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Util class which includes helper methods to fetch the information from
 * Catalog in order to serve the HMS APIs.
 */
public class CatalogHmsAPIHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogHmsAPIHelper.class);
  // we log at info level if the file-metadata load time on the fallback path took more
  // than 100 msec.
  //TODO change this to per directory level value based on empirical average value
  //for various filesystems.
  private static final long FALLBACK_FILE_MD_TIME_WARN_THRESHOLD_MS = 100;

  //TODO Make this individually configurable
  private static final ExecutorService fallbackFdLoaderPool = Executors
      .newFixedThreadPool(20,
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("HMS-Filemetadata-loader-%d").build());

  public static final String IMPALA_TNETWORK_ADDRESSES = "impala:TNetworkAddress";

  /**
   * Helper method to serve a get_table_req() API via the catalog. If isGetColumnStats()
   * is true in the request, we check that the engine is Impala.
   *
   * @param catalog         The catalog instance which caches the table.
   * @param getTableRequest The GetTableRequest object passed by the caller.
   * @return GetTableResult for the given request.
   */
  public static GetTableResult getTableReq(CatalogServiceCatalog catalog,
      String defaultCatalogName, GetTableRequest getTableRequest)
      throws CatalogException, NoSuchObjectException, MetaException {
    checkCatalogName(getTableRequest.getCatName(), defaultCatalogName);
    checkCondition(getTableRequest.getDbName() != null,
        "Database name is null");
    checkCondition(getTableRequest.getTblName() != null,
        "Table name is null");
    String dbName = getTableRequest.getDbName();
    String tblName = getTableRequest.getTblName();
    TableName tableName = new TableName(dbName, tblName);
    GetPartialCatalogObjectRequestBuilder reqBuilder =
        new GetPartialCatalogObjectRequestBuilder()
            .db(dbName)
            .tbl(tblName);
    // Make sure if getColumnStats() is true, the processor engine is set to Impala.
    if (getTableRequest.isGetColumnStats()) {
      checkCondition(getTableRequest.getEngine() != null, "Column stats are requested "
          + "but engine is not set in the request.");
      checkCondition(
          MetastoreShim.IMPALA_ENGINE.equalsIgnoreCase(getTableRequest.getEngine()),
          "Unsupported engine " + getTableRequest.getEngine()
              + " while requesting column statistics of the table " + dbName + "."
              + tblName);
      reqBuilder.wantStatsForAllColums();
    }
    // if the client request has getFileMetadata flag set,
    // we retrieve file descriptors too
    if (getTableRequest.isGetFileMetadata()) {
      reqBuilder.wantFiles();
    }
    if (getTableRequest.isSetValidWriteIdList()) {
      reqBuilder.writeId(getTableRequest.getValidWriteIdList());
    }
    //TODO add table id in the request when client passes it. Also, looks like
    // when HIVE-24662 is resolved, we may not need the table id in this request.
    TGetPartialCatalogObjectResponse response = getPartialCatalogObjResponse(
        catalog, reqBuilder.build(), dbName, tblName,
        HmsApiNameEnum.GET_TABLE_REQ.apiName());
    // As of writing this code, we know that the table which is returned in the response
    // is a copy of table stored in the catalogd. Should this assumption change in the
    // future, we must make sure that we take a deepCopy() of the table before modifying
    // it later below.
    Table retTable = response.table_info.hms_table;
    checkCondition(
        !AcidUtils.isTransactionalTable(retTable.getParameters()) || getTableRequest
            .isSetValidWriteIdList(), "Table " + dbName + "." + tblName
            + " is transactional but it was requested without providing"
            + " validWriteIdList");
    if (getTableRequest.isGetColumnStats()) {
      List<ColumnStatisticsObj> columnStatisticsObjList =
          response.table_info.column_stats;
      checkCondition(columnStatisticsObjList != null,
          "Catalog returned a null ColumnStatisticsObj for table %s", tableName);
      // Set the table level column statistics for this table.
      ColumnStatistics columnStatistics = MetastoreShim.createNewHiveColStats();
      columnStatistics.setStatsDesc(new ColumnStatisticsDesc(true, dbName, tblName));
      for (ColumnStatisticsObj colsStatsObj : columnStatisticsObjList) {
        columnStatistics.addToStatsObj(colsStatsObj);
      }
      retTable.setColStats(columnStatistics);
    }
    if (getTableRequest.isGetFileMetadata()) {
      // set the file-metadata in the response
      checkCondition(response.table_info.partitions != null,
          "File metadata was not returned by catalog");
      checkCondition(response.table_info.partitions.size() == 1,
          "Retrieving file-metadata for partitioned tables must use partition level"
              + " fetch APIs");
      FileMetadata fileMetadata = new FileMetadata();
      if (response.table_info.partitions.get(0).insert_file_descriptors.size() == 0) {
        for (THdfsFileDesc fd : response.table_info.partitions.get(0).file_descriptors) {
          fileMetadata.addToData(fd.file_desc_data);
        }
      } else {
        for (THdfsFileDesc fd :
            response.table_info.partitions.get(0).insert_file_descriptors) {
          fileMetadata.addToData(fd.file_desc_data);
        }
        for (THdfsFileDesc fd :
            response.table_info.partitions.get(0).delete_file_descriptors) {
          fileMetadata.addToData(fd.file_desc_data);
        }
      }
      retTable.setFileMetadata(fileMetadata);
      retTable.setDictionary(getSerializedNetworkAddress(
          response.table_info.network_addresses));
    }
    return new GetTableResult(retTable);
  }

  /**
   * Util method to issue the {@link CatalogServiceCatalog#getPartialCatalogObject} and
   * parse the response to do some sanity checks.
   */
  private static TGetPartialCatalogObjectResponse getPartialCatalogObjResponse(
      CatalogServiceCatalog catalog, TGetPartialCatalogObjectRequest request,
      String dbName, String tblName, String mesg)
      throws CatalogException, NoSuchObjectException {
    TGetPartialCatalogObjectResponse response = catalog
        .getPartialCatalogObject(request, mesg);
    checkCondition(response != null, "Catalog returned a null response");
    if (response.lookup_status == CatalogLookupStatus.DB_NOT_FOUND) {
      throw new NoSuchObjectException("Database " + dbName + " not found");
    }
    if (response.lookup_status == CatalogLookupStatus.TABLE_NOT_FOUND) {
      throw new NoSuchObjectException("Table " + dbName + "." + tblName + " not found");
    }
    checkCondition(response.lookup_status != CatalogLookupStatus.TABLE_NOT_LOADED,
        "Could not load table %s.%s", dbName, tblName);
    checkCondition(response.table_info.hms_table != null,
        "Catalog returned a null table for %s.%s", dbName, tblName);
    return response;
  }

  /**
   * Helper method to get the partitions filtered by a Hive expression.
   *
   * @param catalog         The catalog instance which caches the table.
   * @param defaultCatalog  Currently, custom catalog names are not supported.
   *                        If the catalog is not null it must be same as defaultCatalog.
   * @param request         The request object as received from the HMS client.
   * @param expressionProxy The PartitionExpressionProxy instance which is used to
   *                        deserialize the filter expression.
   * @return PartitionsByExprResult for the given request.
   * @throws CatalogException If there are any internal processing errors (eg. table does
   *                          not exist in Catalog cache) within the context of Catalog.
   * @throws MetaException    If the expression cannot be deserialized using the given
   *                          PartitionExpressionProxy.
   */
  public static PartitionsByExprResult getPartitionsByExpr(CatalogServiceCatalog catalog,
      String defaultCatalog, PartitionsByExprRequest request,
      PartitionExpressionProxy expressionProxy)
      throws CatalogException, NoSuchObjectException, MetaException {
    checkCatalogName(request.getCatName(), defaultCatalog);
    checkCondition(request.getDbName() != null, "Database name is null");
    checkCondition(request.getTblName() != null, "Table name is null");
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    TableName tableName = new TableName(dbName, tblName);
    // TODO we are currently fetching all the partitions metadata here since catalog
    // loads the entire table. Once we have fine-grained loading, we should just get
    // the partitionNames here and then get the partition metadata after the pruning
    // is done.
    GetPartialCatalogObjectRequestBuilder catalogReq =
        new GetPartialCatalogObjectRequestBuilder()
            .db(dbName)
            .tbl(tblName)
            .wantPartitions();
    // set the validWriteIdList if available
    if (request.isSetValidWriteIdList()) {
      catalogReq.writeId(request.getValidWriteIdList());
    }
    // set the table id if available
    if (request.isSetId()) {
      catalogReq.tableId(request.getId());
    }
    TGetPartialCatalogObjectResponse response = getPartialCatalogObjResponse(catalog,
        catalogReq.build(), dbName, tblName,
        HmsApiNameEnum.GET_PARTITION_BY_EXPR.apiName());
    checkCondition(response.table_info.hms_table.getPartitionKeys() != null,
        "%s is not a partitioned table", tableName);
    // create a mapping of the Partition name to the Partition so that we can return the
    // filtered partitions later.
    Map<String, TPartialPartitionInfo> partitionNameToPartInfo = new HashMap<>();
    for (TPartialPartitionInfo partInfo : response.getTable_info().getPartitions()) {
      partitionNameToPartInfo.put(partInfo.getName(), partInfo);
    }
    List<String> filteredPartNames = Lists.newArrayList(partitionNameToPartInfo.keySet());
    Stopwatch st = Stopwatch.createStarted();
    boolean hasUnknownPartitions = expressionProxy
        .filterPartitionsByExpr(response.table_info.hms_table.getPartitionKeys(),
            request.getExpr(), request.getDefaultPartitionName(), filteredPartNames);
    LOG.info("{}/{} partitions were selected for table {} after expression evaluation."
            + " Time taken: {} msec.", filteredPartNames.size(),
        partitionNameToPartInfo.size(), tableName,
        st.stop().elapsed(TimeUnit.MILLISECONDS));
    List<Partition> filteredPartitions = Lists
        .newArrayListWithCapacity(filteredPartNames.size());
    // TODO add file-metadata to Partitions. This would requires changes to HMS API
    // so that request can pass a flag to send back filemetadata
    for (String partName : filteredPartNames) {
      // Note that we are not using String.format arguments here since String.format()
      // throws a java.util.MissingFormatArgumentException for special characters like
      // '%3A' which could be present in the PartitionName.
      checkCondition(partitionNameToPartInfo.containsKey(partName),
          "Could not find partition id for partition name " + partName);
      TPartialPartitionInfo partInfo = partitionNameToPartInfo.get(partName);
      checkCondition(partInfo != null,
          "Catalog did not return the partition " + partName + " for table " + tableName,
          partName, tableName);
      filteredPartitions.add(partitionNameToPartInfo.get(partName).getHms_partition());
    }
    // confirm if the number of partitions is equal to number of filtered ids
    checkCondition(filteredPartNames.size() == filteredPartitions.size(),
        "Unexpected number of partitions received. Expected %s got %s",
        filteredPartNames.size(), filteredPartitions.size());
    PartitionsByExprResult result = new PartitionsByExprResult();
    result.setPartitions(filteredPartitions);
    result.setHasUnknownPartitions(hasUnknownPartitions);
    return result;
  }

  /**
   * Throws a {@code CatalogException} with the given message string and arguments if the
   * condition is False.
   *
   * @param condition throws CatalogException when the condition is False.
   * @param msg       msg compatible with {@code String.format()} format.
   * @param args      args for the {@code String.format()} to used as message in the
   *                  thrown exception.
   * @throws CatalogException if condition is False.
   */
  private static void checkCondition(boolean condition, String msg, Object... args)
      throws CatalogException {
    if (condition) return;
    throw new CatalogException(String.format(msg, args));
  }

  /**
   * Catalog service does not support {@link org.apache.hadoop.hive.metastore.api.Catalog}
   * currently. This method validates the name of the given catalog with default Catalog
   * and throws a {@link MetaException} if it doesn't match.
   */
  public static void checkCatalogName(String catalogName, String defaultCatalogName)
      throws MetaException {
    if (catalogName == null) return;
    try {
      checkCondition(defaultCatalogName != null, "Default catalog name is null");
      checkCondition(defaultCatalogName.equalsIgnoreCase(catalogName),
          "Catalog service does not support non-default catalogs. Expected %s got %s",
          defaultCatalogName, catalogName);
    } catch (CatalogException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new MetaException(ex.getMessage());
    }
  }

  /**
   * Helper method to return a list of Partitions filtered by names. Currently partition
   * level column statistics cannot be requested and this method throws an exception if
   * the request has get_col_stats parameter set. If the request has {@code
   * getFileMetadata} set the returned response will include the file-metadata as well.
   */
  public static GetPartitionsByNamesResult getPartitionsByNames(
      CatalogServiceCatalog catalog, Configuration serverConf,
      GetPartitionsByNamesRequest request)
      throws CatalogException, NoSuchObjectException, MetaException {
    // in case of GetPartitionsByNamesRequest there is no explicit catalogName
    // field and hence the dbName is prepended to the database name.
    // TODO this needs to be fixed in HMS APIs so that we have explicit catalog field.
    String catAnddbName = request.getDb_name();
    String tblName = request.getTbl_name();
    checkCondition(!Strings.isNullOrEmpty(catAnddbName),
        "Database name is empty or null");
    checkCondition(!Strings.isNullOrEmpty(tblName), "Table name is empty or null");
    String[] parsedCatDbName = MetaStoreUtils
        .parseDbName(request.getDb_name(), serverConf);
    checkCondition(parsedCatDbName.length == 2,
        "Unexpected error during parsing the catalog and database name %s", catAnddbName);
    checkCatalogName(parsedCatDbName[0], MetaStoreUtils.getDefaultCatalog(serverConf));
    String dbName = parsedCatDbName[1];
    //TODO partition granularity column statistics are not supported in catalogd currently
    checkCondition(!request.isGet_col_stats(),
        "Partition level column statistics are not supported in catalog");
    GetPartialCatalogObjectRequestBuilder requestBuilder =
        new GetPartialCatalogObjectRequestBuilder()
            .db(dbName)
            .tbl(tblName)
            .wantPartitions();
    // get the file metadata if the request has it set.
    if (request.isGetFileMetadata()) {
      requestBuilder.wantFiles();
    }
    if (request.isSetValidWriteIdList()) {
      requestBuilder.writeId(request.getValidWriteIdList());
    }
    //TODO add table id in the request when client passes it. Also, looks like
    // when HIVE-24662 is resolved, we may not need the table id in this request.
    TGetPartialCatalogObjectResponse response = getPartialCatalogObjResponse(catalog,
        requestBuilder.build(), dbName, tblName,
        HmsApiNameEnum.GET_PARTITION_BY_NAMES.apiName());
    checkCondition(response.table_info.hms_table.getPartitionKeys() != null,
        "%s.%s is not a partitioned table", dbName, tblName);
    checkCondition(
        !request.isGetFileMetadata() || response.table_info.network_addresses != null,
        "Network addresses were not returned for %s.%s", dbName, tblName);
    checkCondition(
        !AcidUtils.isTransactionalTable(response.table_info.hms_table.getParameters())
            || request.isSetValidWriteIdList(), "Table " + dbName + "." + tblName
            + " is a transactional table but partitions were requested without "
            + "providing validWriteIdList");
    // create a mapping of the Partition name to the Partition so that we can return the
    // filtered partitions later.
    Set<String> requestedNames = new HashSet<>(request.getNames());
    List<Partition> retPartitions = new ArrayList<>();
    // filter by names
    for (TPartialPartitionInfo partInfo : response.getTable_info().getPartitions()) {
      if (requestedNames.contains(partInfo.getName())) {
        // as of writing this code, we know that the partInfo object has a copy of the
        // the partition which is stored in the catalogd instead of the reference to the
        // actual partition object of the catalogd. Hence modifying it below is okay.
        // Should this assumption change in the future, we must make a deepCopy of the
        // partition object here to make sure that we don't modify the state of the
        // partition in the catalogd.
        Partition part = partInfo.getHms_partition();
        if (request.isGetFileMetadata()) {
          FileMetadata fileMetadata = new FileMetadata();
          checkCondition(partInfo.file_descriptors != null,
              "Catalog did not return file descriptors for partition %s of table %s.%s",
              partInfo.getName(), dbName, tblName);
          if (partInfo.insert_file_descriptors.isEmpty()) {
            for (THdfsFileDesc fd : partInfo.file_descriptors) {
              fileMetadata.addToData(fd.file_desc_data);
            }
          } else {
            for (THdfsFileDesc fd : partInfo.insert_file_descriptors) {
              fileMetadata.addToData(fd.file_desc_data);
            }
            for (THdfsFileDesc fd : partInfo.delete_file_descriptors) {
              fileMetadata.addToData(fd.file_desc_data);
            }
          }
          part.setFileMetadata(fileMetadata);
        }
        retPartitions.add(part);
      }
    }
    // HMS API returns partitions sorted by partition name, however this is just a
    // semantic behavior. We don't have any evidence of clients relying on the sortedness
    // of returned partitions. Hence, we optimistically return the partitions without
    // sorting by names here.
    GetPartitionsByNamesResult result = new GetPartitionsByNamesResult(retPartitions);
    if (request.isGetFileMetadata()) {
      result.setDictionary(getSerializedNetworkAddress(
          response.table_info.network_addresses));
    }
    return result;
  }

  /**
   * Util method to serialize a given list of {@link TNetworkAddress} into a {@link
   * ObjectDictionary}.
   */
  public static ObjectDictionary getSerializedNetworkAddress(
      List<TNetworkAddress> networkAddresses) throws CatalogException {
    checkCondition(networkAddresses != null, "Network addresses is null");
    // we assume that this method is only called when we want to return the file-metadata
    // in the response in which case there should always a valid ObjectDictionary to be
    // returned.
    ObjectDictionary result = new ObjectDictionary(Maps.newHashMap());
    if (networkAddresses.isEmpty()) return result;
    List<ByteBuffer> serializedAddresses = new ArrayList<>();
    TSerializer serializer = null;
    try {
      serializer = new TSerializer(new TCompactProtocol.Factory());
    } catch (TException e) {
      throw new CatalogException("Could not create serializer. " + e.getMessage());
    }
    for (TNetworkAddress networkAddress : networkAddresses) {
      byte[] serializedNetAddress;
      try {
        serializedNetAddress = serializer.serialize(networkAddress);
      } catch (TException tException) {
        throw new CatalogException(
            "Could not serialize network address " + networkAddress.hostname + ":"
                + networkAddress.port, tException);
      }
      serializedAddresses.add(ByteBuffer.wrap(serializedNetAddress));
    }
    result.putToValues(IMPALA_TNETWORK_ADDRESSES, serializedAddresses);
    return result;
  }

  /**
   * This method computes the file-metadata directly from filesystem and sets it in the
   * Table object of the provided GetTableResult. The file-metadata is loaded using a
   * common static thread pool and is consistent with the given ValidTxnList and
   * ValidWriteIdList.
   *
   * @throws MetaException in case there were errors while loading file-metadata.
   */
  public static void loadAndSetFileMetadataFromFs(@Nullable ValidTxnList validTxnList,
      @Nullable ValidWriteIdList writeIdList, GetTableResult result)
      throws MetaException {
    try {
      Stopwatch sw = Stopwatch.createStarted();
      Table tbl = result.getTable();
      checkCondition(tbl != null, "Table is null");
      checkCondition(tbl.getSd() != null && tbl.getSd().getLocation() != null,
          "Cannot get the location of table %s.%s", tbl.getDbName(), tbl.getTableName());
      Path tblPath = new Path(tbl.getSd().getLocation());
      // since this table doesn't exist in catalogd we compute the network addresses
      // for the files which are being returned.
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      FileMetadataLoader fmLoader = new FileMetadataLoader(tblPath, true,
          Collections.EMPTY_LIST, hostIndex, validTxnList, writeIdList);
      boolean success = getFileMetadata(Arrays.asList(fmLoader));
      checkCondition(success,
          "Could not load file-metadata for table %s.%s. See catalogd log for details",
          tbl.getDbName(), tbl.getTableName());
      FileMetadata fileMetadata = new FileMetadata();
      for (FileDescriptor fd : fmLoader.getLoadedFds()) {
        fileMetadata.addToData(fd.toThrift().file_desc_data);
      }
      tbl.setFileMetadata(fileMetadata);
      tbl.setDictionary(getSerializedNetworkAddress(hostIndex.getList()));
      long timeTaken = sw.stop().elapsed(TimeUnit.MILLISECONDS);
      if (timeTaken > FALLBACK_FILE_MD_TIME_WARN_THRESHOLD_MS) {
        LOG.warn("Loading the filemetadata for table {}.{} on the fallback path. Time "
            + "taken: {} msec", tbl.getDbName(), tbl.getTableName(), timeTaken);
      } else {
        LOG.debug("Loading the filemetadata for table {}.{} on the fallback path. Time "
            + "taken: {} msec", tbl.getDbName(), tbl.getTableName(), timeTaken);
      }
    } catch (Exception ex) {
      LOG.error("Unexpected error when loading filemetadata", ex);
      throw new MetaException(
          "Could not load filemetadata. Cause " + ex.getMessage());
    }
  }

  /**
   * Sets the file metadata for given partitions from the file-system. In case of
   * transactional tables it uses the given ValidTxnList and ValidWriteIdList to compute
   * the snapshot of the file-metadata.
   */
  public static void loadAndSetFileMetadataFromFs(@Nullable ValidTxnList txnList,
      @Nullable ValidWriteIdList writeIdList,
      GetPartitionsByNamesResult getPartsResult) throws MetaException {
    if (getPartsResult.getPartitionsSize() == 0) return;
    final String dbName = getPartsResult.getPartitions().get(0).getDbName();
    final String tblName = getPartsResult.getPartitions().get(0).getTableName();
    try {
      Stopwatch sw = Stopwatch.createStarted();
      Map<Partition, FileMetadataLoader> fileMdLoaders = new HashMap<>();
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      for (Partition part : getPartsResult.getPartitions()) {
        checkCondition(part.getSd() != null && part.getSd().getLocation() != null,
            "Could not get the location for partition %s of table %s.%s",
            part.getValues(), part.getDbName(), part.getTableName());
        Path partPath = new Path(part.getSd().getLocation());
        fileMdLoaders.put(part,
            new FileMetadataLoader(partPath, true, Collections.EMPTY_LIST, hostIndex,
                txnList, writeIdList));
      }
      boolean success = getFileMetadata(fileMdLoaders.values());
      checkCondition(success,
          "Could not load file-metadata for %s partitions of table %s.%s. See "
              + "catalogd log for details", getPartsResult.getPartitionsSize(), dbName,
          tblName);
      for (Entry<Partition, FileMetadataLoader> entry : fileMdLoaders.entrySet()) {
        FileMetadata filemetadata = new FileMetadata();
        for (FileDescriptor fd : entry.getValue().getLoadedFds()) {
          filemetadata.addToData(fd.toThrift().file_desc_data);
        }
        entry.getKey().setFileMetadata(filemetadata);
      }
      getPartsResult.setDictionary(getSerializedNetworkAddress(hostIndex.getList()));
      long timeTaken = sw.stop().elapsed(TimeUnit.MILLISECONDS);
      if (timeTaken > FALLBACK_FILE_MD_TIME_WARN_THRESHOLD_MS) {
        LOG.info("Loading the file metadata for {} partitions of table {}.{} on the "
                + "fallback path. Time taken: {} msec",
            getPartsResult.getPartitionsSize(), dbName, tblName, timeTaken);
      } else {
        LOG.debug("Loading the file metadata for {} partitions of table {}.{} on the "
                + "fallback path. Time taken: {} msec",
            getPartsResult.getPartitionsSize(), dbName, tblName, timeTaken);
      }
    } catch (CatalogException ex) {
      LOG.error(
          "Unexpected error when loading file-metadata for partitions of table {}.{}",
          dbName, tblName, ex);
      throw new MetaException("Could not load file metadata. Cause " + ex.getMessage());
    }
  }

  /**
   * Loads the file-metadata in parallel using the common thread pool {@code
   * fallbackFdLoaderPool}
   *
   * @param loaders The FileMetadataLoader objects corresponding for each path.
   * @return false if there were errors, else returns true.
   */
  private static boolean getFileMetadata(Collection<FileMetadataLoader> loaders) {
    List<Pair<Path, Future<Void>>> futures = new ArrayList<>(loaders.size());
    for (FileMetadataLoader fmdLoader : loaders) {
      futures.add(new Pair<>(fmdLoader.getPartDir(), fallbackFdLoaderPool.submit(() -> {
        fmdLoader.load();
        return null;
      })));
    }
    int numberOfErrorsToLog = 100;
    int errors = 0;
    for (Pair<Path, Future<Void>> pair : futures) {
      try {
        pair.second.get();
      } catch (InterruptedException | ExecutionException e) {
        errors++;
        if (errors < numberOfErrorsToLog) {
          LOG.error("Could not load file-metadata for path {}", pair.first, e);
        }
      }
    }
    if (errors > 0 && (numberOfErrorsToLog - errors) > 0) {
      LOG.error("{} loading errors were not logged. Only logged the first {} errors",
          numberOfErrorsToLog - errors, numberOfErrorsToLog);
    }
    return errors == 0;
  }
}

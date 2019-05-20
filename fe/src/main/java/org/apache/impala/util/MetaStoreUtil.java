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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import org.apache.impala.thrift.TColumn;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Utility methods for interacting with the Hive Metastore.
 */
public class MetaStoreUtil {
  private static final Logger LOG = Logger.getLogger(MetaStoreUtil.class);

  // Maximum comment length, e.g., for columns, that can be stored in the HMS.
  // This number is a lower bound of the constraint set in the HMS DB schema,
  // because the constraint varies among different backing databases, e.g.,
  // for Postgres it is 4000, but for most other databases it is 256.
  public static final int CREATE_MAX_COMMENT_LENGTH = 256;

  // The longest strings Hive accepts for [serde] property keys.
  public static final int MAX_PROPERTY_KEY_LENGTH = 256;

  // The longest strings Hive accepts for [serde] property values.
  public static final int MAX_PROPERTY_VALUE_LENGTH = 4000;

  // Maximum owner length. The owner can be user or role.
  // https://github.com/apache/hive/blob/13fbae57321f3525cabb326df702430d61c242f9/standalone-metastore/src/main/resources/package.jdo#L63
  public static final int MAX_OWNER_LENGTH = 128;

  // The default maximum number of partitions to fetch from the Hive metastore in one
  // RPC.
  private static final short DEFAULT_MAX_PARTITIONS_PER_RPC = 1000;

  // The maximum number of partitions to fetch from the metastore in one RPC.
  // Read from the 'hive.metastore.batch.retrieve.table.partition.max' Hive configuration
  // and defaults to DEFAULT_MAX_PARTITION_BATCH_SIZE if the value is not present in the
  // Hive configuration.
  private static short maxPartitionsPerRpc_ = DEFAULT_MAX_PARTITIONS_PER_RPC;

  // The configuration key that Hive uses to set the null partition key value.
  public static final String NULL_PARTITION_KEY_VALUE_CONF_KEY =
      "hive.exec.default.partition.name";
  // The default value for the above configuration key.
  public static final String DEFAULT_NULL_PARTITION_KEY_VALUE =
      "__HIVE_DEFAULT_PARTITION__";

  // The configuration key represents thrift URI for the remote Hive Metastore.
  public static final String HIVE_METASTORE_URIS_KEY = "hive.metastore.uris";
  // The default value for the above configuration key.
  public static final String DEFAULT_HIVE_METASTORE_URIS = "";

  static {
    // Get the value from the Hive configuration, if present.
    HiveConf hiveConf = new HiveConf(HdfsTable.class);
    String strValue =
        hiveConf.get(MetastoreShim.metastoreBatchRetrieveObjectsMaxConfigKey());
    if (strValue != null) {
      try {
        maxPartitionsPerRpc_ = Short.parseShort(strValue);
      } catch (NumberFormatException e) {
        LOG.error("Error parsing max partition batch size from HiveConfig: ", e);
      }
    }
    if (maxPartitionsPerRpc_ <= 0) {
      LOG.error(String.format("Invalid value for max partition batch size: %d. Using " +
          "default: %d", maxPartitionsPerRpc_, DEFAULT_MAX_PARTITIONS_PER_RPC));
      maxPartitionsPerRpc_ = DEFAULT_MAX_PARTITIONS_PER_RPC;
    }
  }

  /**
   * Return the value that Hive is configured to use for NULL partition key values.
   */
  public static String getNullPartitionKeyValue(IMetaStoreClient client)
      throws ConfigValSecurityException, TException {
    return client.getConfigValue(
        NULL_PARTITION_KEY_VALUE_CONF_KEY, DEFAULT_NULL_PARTITION_KEY_VALUE);
  }

  /**
   * Return the value of thrift URI for the remote Hive Metastore.
   */
  public static String getHiveMetastoreUrisKeyValue(IMetaStoreClient client)
      throws ConfigValSecurityException, TException {
    return client.getConfigValue(
        HIVE_METASTORE_URIS_KEY, DEFAULT_HIVE_METASTORE_URIS);
  }

  /**
   * Return the value set for the given config in the metastore.
   */
  public static String getMetastoreConfigValue(
      IMetaStoreClient client, String config, String defaultVal) throws TException {
    return client.getConfigValue(config, defaultVal);
  }

  /**
   * Fetches all partitions for a table in batches, with each batch containing at most
   * 'maxPartsPerRpc' partitions. Returns a List containing all fetched Partitions.
   * Will throw a MetaException if existing partitions are dropped while a fetch is in
   * progress. To help protect against this, the operation can be retried if there is
   * a MetaException by setting the "numRetries" parameter.
   * Failures due to thrift exceptions (TExceptions) are not retried because they
   * generally mean the connection is broken or has timed out. The HiveClient supports
   * configuring retires at the connection level so it can be enabled independently.
   */
  public static List<org.apache.hadoop.hive.metastore.api.Partition> fetchAllPartitions(
      IMetaStoreClient client, String dbName, String tblName, int numRetries)
      throws MetaException, TException {
    Preconditions.checkArgument(numRetries >= 0);
    int retryAttempt = 0;
    while (true) {
      try {
        // First, get all partition names that currently exist.
        List<String> partNames = client.listPartitionNames(dbName, tblName, (short) -1);
        return MetaStoreUtil.fetchPartitionsByName(client, partNames, dbName, tblName);
      } catch (MetaException e) {
        // Only retry for MetaExceptions, since TExceptions could indicate a broken
        // connection which we can't recover from by retrying.
        if (retryAttempt < numRetries) {
          LOG.error(String.format("Error fetching partitions for table: %s.%s. " +
              "Retry attempt: %d/%d", dbName, tblName, retryAttempt, numRetries), e);
          ++retryAttempt;
          // TODO: Sleep for a bit?
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Given a List of partition names, fetches the matching Partitions from the HMS
   * in batches. Each batch will contain at most 'maxPartsPerRpc' partitions.
   * Returns a List containing all fetched Partitions.
   * Will throw a MetaException if any partitions in 'partNames' do not exist.
   */
  public static List<Partition> fetchPartitionsByName(
      IMetaStoreClient client, List<String> partNames, String dbName, String tblName)
      throws MetaException, TException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Fetching %d partitions for: %s.%s using partition " +
          "batch size: %d", partNames.size(), dbName, tblName, maxPartitionsPerRpc_));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> fetchedPartitions =
        Lists.newArrayList();
    // Fetch the partitions in batches.
    for (int i = 0; i < partNames.size(); i += maxPartitionsPerRpc_) {
      // Get a subset of partition names to fetch.
      List<String> partsToFetch =
          partNames.subList(i, Math.min(i + maxPartitionsPerRpc_, partNames.size()));
      // Fetch these partitions from the metastore.
      fetchedPartitions.addAll(
          client.getPartitionsByNames(dbName, tblName, partsToFetch));
    }
    return fetchedPartitions;
  }

  /**
   * Checks that a given 'property' is short enough for HMS to handle. If not, throws an
   * 'AnalysisException' with 'name' as its prefix.
   */
  public static void checkShortProperty(String name, String property, int length)
      throws AnalysisException {
    if (property.length() > length) {
      throw new AnalysisException(
          name + " length must be <= " + length + ": " + property.length());
    }
  }

  /**
   * Checks that each key and value in a property map is short enough for HMS to handle.
   * If not, An 'AnalysisException' is thrown with 'mapName' as its prefix.
   */
  public static void checkShortPropertyMap(
      String mapName, Map<String, String> propertyMap) throws AnalysisException {
    if (null != propertyMap) {
      for (Map.Entry<String, String> property : propertyMap.entrySet()) {
        checkShortProperty(mapName + " key", property.getKey(), MAX_PROPERTY_KEY_LENGTH);
        checkShortProperty(
            mapName + " value", property.getValue(), MAX_PROPERTY_VALUE_LENGTH);
      }
    }
  }

  /**
   * Does a case-insensitive search for 'propertyKey' in 'propertyMap'. If a match is
   * found, the matched key is returned, otherwise null is returned. 'propertyMap' and
   * 'propertyKey' must not be null.
   */
  public static String findTblPropKeyCaseInsensitive(Map<String, String> propertyMap,
      String propertyKey) {
    Preconditions.checkNotNull(propertyMap);
    Preconditions.checkNotNull(propertyKey);
    for (String key : propertyMap.keySet()) {
      if (key != null && key.equalsIgnoreCase(propertyKey)) return key;
    }
    return null;
  }

  /**
   * Returns a copy of the comma-separated list of values 'inputCsv', with all occurences
   * of 'toReplace' replaced with value 'replaceWith'.
   */
  public static String replaceValueInCsvList(String input, String toReplace,
      String replaceWith) {
    Iterable<String> inputList =
        Splitter.on(",").trimResults().omitEmptyStrings().split(input);
    List<String> outputList = Lists.newArrayList();
    for (String elem : inputList) {
      if (elem.equalsIgnoreCase(toReplace)) {
        outputList.add(replaceWith);
      } else {
        outputList.add(elem);
      }
    }
    return Joiner.on(",").join(outputList);
  }

  /**
   * Returns a copy of the comma-separated list of values 'inputCsv', with all occurences
   * of 'toRemove' removed.
   */
  public static String removeValueFromCsvList(String inputCsv, String toRemove) {
    Iterable<String> inputList =
        Splitter.on(",").trimResults().omitEmptyStrings().split(inputCsv);
    List<String> outputList = Lists.newArrayList();
    for (String elem : inputList) {
      if (!elem.equalsIgnoreCase(toRemove)) outputList.add(elem);
    }
    return Joiner.on(",").join(outputList);
  }

  /**
   * Returns the intersection of the comma-separated list of values 'leftCsv' with the
   * names of the columns in 'rightCols'. The return value is a comma-separated list.
   */
  public static String intersectCsvListWithColumNames(String leftCsv,
      List<TColumn> rightCols) {
    Iterable<String> leftCols =
        Splitter.on(",").trimResults().omitEmptyStrings().split(leftCsv);
    HashSet<String> rightColNames = Sets.newHashSet();
    for (TColumn c : rightCols) rightColNames.add(c.getColumnName().toLowerCase());
    List<String> outputList = Lists.newArrayList();
    for (String leftCol : leftCols) {
      if (rightColNames.contains(leftCol.toLowerCase())) outputList.add(leftCol);
    }
    return Joiner.on(",").join(outputList);
  }

  public static List<String> getPartValsFromName(Table msTbl, String partName)
      throws MetaException, CatalogException {
    Preconditions.checkNotNull(msTbl);
    LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);
    List<String> partVals = Lists.newArrayList();
    for (FieldSchema field: msTbl.getPartitionKeys()) {
      String key = field.getName();
      String val = hm.get(key);
      if (val == null) {
        throw new CatalogException("Incomplete partition name - missing " + key);
      }
      partVals.add(val);
    }
    return partVals;
  }
}

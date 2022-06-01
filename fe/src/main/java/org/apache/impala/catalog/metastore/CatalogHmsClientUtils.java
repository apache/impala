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

package org.apache.impala.catalog.metastore;

import static org.apache.impala.catalog.CatalogHmsAPIHelper.IMPALA_TNETWORK_ADDRESSES;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FileMetadata;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.ObjectDictionary;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogHmsAPIHelper;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol.Factory;

public class CatalogHmsClientUtils {

  /**
   * Util method useful to deserialize the
   * {@link FileMetadata} in the {@link GetPartitionsByNamesResult} into a list of
   * {@link FileDescriptor} objects. The returned Map contains a mapping of partitions
   * to its List of FileDescriptors.
   * @param getPartitionsResult The getPartitionsByNamesResult as returned by the HMS API
   * {@code getPartitionsByNames}. If this param does not contain fileMetadata the method
   *                            throws an exception.
   * @param hostIndex The HostIndex to be used to clone the FileDescriptors before
   *                  returning. This is useful to map the FileDescriptors to an existing
   *                  HostIndex which is available for the table.
   * @return Map of Partition to its FileDescriptors. Note that list can be empty in
   * case there are no files in a partition.
   * @throws CatalogException If there are any deserialization errors.
   */
  public static Map<Partition, List<FileDescriptor>> extractFileDescriptors(
      GetPartitionsByNamesResult getPartitionsResult, ListMap<TNetworkAddress> hostIndex)
      throws CatalogException {
    // if there are no partitions in the result, return early
    if (getPartitionsResult.getPartitionsSize() == 0) return new HashMap<>(0);
    Preconditions
        .checkArgument(getPartitionsResult.isSetDictionary(), "Host info is unavailable");
    List<TNetworkAddress> hostInfo = deserializeNetworkAddresses(
        getPartitionsResult.getDictionary());
    Map<Partition, List<FileDescriptor>> fds = Maps
        .newHashMapWithExpectedSize(getPartitionsResult.getPartitionsSize());
    for (Partition part : getPartitionsResult.getPartitions()) {
      Preconditions.checkArgument(part.isSetFileMetadata(),
          "Filemetadata is not set for partition with values " + part.getValues());
      FileMetadata fileMetadata = part.getFileMetadata();
      List<FileDescriptor> partitionFds = Lists
          .newArrayListWithCapacity(fileMetadata.getDataSize());
      if (fileMetadata.getData() != null) {
        for (ByteBuffer data : fileMetadata.getData()) {
          FileDescriptor fd = FileDescriptor.fromThrift(new THdfsFileDesc(data));
          fd.cloneWithNewHostIndex(hostInfo, hostIndex);
          partitionFds.add(fd);
        }
      }
      fds.put(part, partitionFds);
    }
    return fds;
  }

  /**
   * Util method to extract the FileDescriptors from the Table returned
   * from HMS. The method expects that the result has FileMetadata set in the table
   * along with the Host network addresses serialized in the table dictionary.
   * @param tbl The table returned from HMS API getTableReq
   * @param hostIndex The hostIndex of the table which is used to clone the returned
   *                  FileDescriptors.
   * @return List of FileDescriptors for the table.
   * @throws CatalogException
   */
  public static List<FileDescriptor> extractFileDescriptors(Table tbl,
      ListMap<TNetworkAddress> hostIndex) throws CatalogException {
    String fullTblName =
        tbl.getDbName() + "." + tbl.getTableName();
    Preconditions.checkArgument(tbl.isSetDictionary(),
        "Host info is not available in the table " + fullTblName);
    List<TNetworkAddress> hostInfo = deserializeNetworkAddresses(
        tbl.getDictionary());
    Preconditions.checkArgument(tbl.isSetFileMetadata(),
        "Filemetadata is not set for table " + fullTblName);
    FileMetadata fileMetadata = tbl.getFileMetadata();
    // it is possible that there are no files in the table.
    if (fileMetadata.getData() == null) return Collections.emptyList();
    List<FileDescriptor> tableFds = Lists
        .newArrayListWithCapacity(fileMetadata.getDataSize());
    for (ByteBuffer data : fileMetadata.getData()) {
      FileDescriptor fd = FileDescriptor.fromThrift(new THdfsFileDesc(data));
      fd.cloneWithNewHostIndex(hostInfo, hostIndex);
      tableFds.add(fd);
    }
    return tableFds;
  }

  /**
   * Util method to deserialize a given {@link ObjectDictionary} object into a list of
   * {@link TNetworkAddress}. This is used to deserialize the output of
   * {@link CatalogHmsAPIHelper#getSerializedNetworkAddress(List)}.
   * @param dictionary
   * @return list of deserialized TNetworkAddress
   * @throws CatalogException in case of deserialization errors.
   */
  private static List<TNetworkAddress> deserializeNetworkAddresses(
      ObjectDictionary dictionary) throws CatalogException {
    if (dictionary == null) return null;
    if (dictionary.getValuesSize() == 0) return Collections.EMPTY_LIST;
    if (!dictionary.getValues()
        .containsKey(IMPALA_TNETWORK_ADDRESSES)) {
      throw new CatalogException("Key " + IMPALA_TNETWORK_ADDRESSES + " not found");
    }
    List<ByteBuffer> serializedNetAddresses = dictionary.getValues()
        .get(IMPALA_TNETWORK_ADDRESSES);
    List<TNetworkAddress> networkAddresses = Lists
        .newArrayListWithCapacity(serializedNetAddresses.size());
    int index = 0;
    TDeserializer deserializer = null;
    try {
      deserializer = new TDeserializer(new Factory());
    } catch (TException e) {
      throw new CatalogException("Could not create deserializer. " + e.getMessage());
    }
    for (ByteBuffer serializedData : serializedNetAddresses) {
      TNetworkAddress networkAddress = new TNetworkAddress();
      try {
        deserializer.deserialize(networkAddress, serializedData.array());
        networkAddresses.add(networkAddress);
        index++;
      } catch (TException tException) {
        throw new CatalogException(
            "Could not deserialize network address at position " + index);
      }
    }
    return networkAddresses;
  }
}

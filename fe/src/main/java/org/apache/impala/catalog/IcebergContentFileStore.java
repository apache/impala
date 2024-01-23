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

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.fb.FbIcebergDataFileFormat;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TIcebergContentFileStore;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

/**
 * Helper class for storing Iceberg file descriptors. It stores data and delete files
 * separately, while also storing file descriptors belonging to earlier snapshots.
 */
public class IcebergContentFileStore {

  // Auxiliary class for holding file descriptors in both a map and a list.
  private static class MapListContainer {
    // Key is the ContentFile path hash, value is FileDescriptor transformed from DataFile
    private final Map<String, FileDescriptor> fileDescMap_ = new HashMap<>();
    private final List<FileDescriptor> fileDescList_ = new ArrayList<>();

    // Adds a file to the map. If this is a new entry, then add it to the list as well.
    // Return true if 'desc' was a new entry.
    public boolean add(String pathHash, FileDescriptor desc) {
      if (fileDescMap_.put(pathHash, desc) == null) {
        fileDescList_.add(desc);
        return true;
      }
      return false;
    }

    public FileDescriptor get(String pathHash) {
      return fileDescMap_.get(pathHash);
    }

    public long getNumFiles() {
      return fileDescList_.size();
    }

    List<FileDescriptor> getList() { return fileDescList_; }

    // It's enough to only convert the map part to thrift.
    Map<String, THdfsFileDesc> toThrift() {
      Map<String, THdfsFileDesc> ret = new HashMap<>();
      for (Map.Entry<String, HdfsPartition.FileDescriptor> entry :
          fileDescMap_.entrySet()) {
        ret.put(entry.getKey(), entry.getValue().toThrift());
      }
      return ret;
    }

    static MapListContainer fromThrift(Map<String, THdfsFileDesc> thriftMap,
        List<TNetworkAddress> networkAddresses, ListMap<TNetworkAddress> hostIndex) {
      MapListContainer ret = new MapListContainer();
      for (Map.Entry<String, THdfsFileDesc> entry : thriftMap.entrySet()) {
        FileDescriptor fd = FileDescriptor.fromThrift(entry.getValue());
        Preconditions.checkNotNull(fd);
        if (networkAddresses != null) {
          Preconditions.checkNotNull(hostIndex);
          fd = fd.cloneWithNewHostIndex(networkAddresses, hostIndex);
        }
        ret.add(entry.getKey(), fd);
      }
      return ret;
    }
  }

  // Separate map-list containers for the different content files.
  private MapListContainer dataFilesWithoutDeletes_ = new MapListContainer();
  private MapListContainer dataFilesWithDeletes_ = new MapListContainer();
  private MapListContainer positionDeleteFiles_ = new MapListContainer();
  private MapListContainer equalityDeleteFiles_ = new MapListContainer();

  // Caches file descriptors loaded during time-travel queries.
  private final ConcurrentMap<String, FileDescriptor> oldFileDescMap_ =
      new ConcurrentHashMap<>();

  // Flags to indicate file formats used in the table.
  private boolean hasAvro_ = false;
  private boolean hasOrc_ = false;
  private boolean hasParquet_ = false;

  public IcebergContentFileStore() {}

  public void addDataFileWithoutDeletes(String pathHash, FileDescriptor desc) {
    if (dataFilesWithoutDeletes_.add(pathHash, desc)) {
      updateFileFormats(desc);
    }
  }

  public void addDataFileWithDeletes(String pathHash, FileDescriptor desc) {
    if (dataFilesWithDeletes_.add(pathHash, desc)) {
      updateFileFormats(desc);
    }
  }

  public void addPositionDeleteFile(String pathHash, FileDescriptor desc) {
    if (positionDeleteFiles_.add(pathHash, desc)) {
      updateFileFormats(desc);
    }
  }

  public void addEqualityDeleteFile(String pathHash, FileDescriptor desc) {
    Preconditions.checkState(
        desc.getFbFileMetadata().icebergMetadata().equalityFieldIdsLength() > 0);
    if (equalityDeleteFiles_.add(pathHash, desc)) updateFileFormats(desc);
  }

  // This is only invoked during time travel, when we are querying a snapshot that has
  // data files which have been removed since.
  public void addOldFileDescriptor(String pathHash, FileDescriptor desc) {
    oldFileDescMap_.put(pathHash, desc);
  }

  public FileDescriptor getDataFileDescriptor(String pathHash) {
    FileDescriptor desc = dataFilesWithoutDeletes_.get(pathHash);
    if (desc != null) return desc;
    return dataFilesWithDeletes_.get(pathHash);
  }

  public FileDescriptor getDeleteFileDescriptor(String pathHash) {
    FileDescriptor ret = positionDeleteFiles_.get(pathHash);
    if (ret != null) return ret;
    return equalityDeleteFiles_.get(pathHash);
  }

  public FileDescriptor getOldFileDescriptor(String pathHash) {
    return oldFileDescMap_.get(pathHash);
  }

  public List<FileDescriptor> getDataFilesWithoutDeletes() {
    return dataFilesWithoutDeletes_.getList();
  }

  public List<FileDescriptor> getDataFilesWithDeletes() {
    return dataFilesWithDeletes_.getList();
  }

  public List<FileDescriptor> getPositionDeleteFiles() {
    return positionDeleteFiles_.getList();
  }

  public List<FileDescriptor> getEqualityDeleteFiles() {
    return equalityDeleteFiles_.getList();
  }

  public long getNumFiles() {
    return dataFilesWithoutDeletes_.getNumFiles() +
           dataFilesWithDeletes_.getNumFiles() +
           positionDeleteFiles_.getNumFiles() +
           equalityDeleteFiles_.getNumFiles();
  }

  public Iterable<FileDescriptor> getAllFiles() {
    return Iterables.concat(
        dataFilesWithoutDeletes_.getList(),
        dataFilesWithDeletes_.getList(),
        positionDeleteFiles_.getList(),
        equalityDeleteFiles_.getList());
  }

  public Iterable<FileDescriptor> getAllDataFiles() {
    return Iterables.concat(
        dataFilesWithoutDeletes_.getList(),
        dataFilesWithDeletes_.getList());
  }

  public boolean hasAvro() { return hasAvro_; }
  public boolean hasOrc() { return hasOrc_; }
  public boolean hasParquet() { return hasParquet_; }

  private void updateFileFormats(FileDescriptor desc) {
    byte fileFormat = desc.getFbFileMetadata().icebergMetadata().fileFormat();
    if (fileFormat == FbIcebergDataFileFormat.PARQUET) {
      hasParquet_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.ORC) {
      hasOrc_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.AVRO) {
      hasAvro_ = true;
    }
  }

  public TIcebergContentFileStore toThrift() {
    TIcebergContentFileStore ret = new TIcebergContentFileStore();
    ret.setPath_hash_to_data_file_without_deletes(dataFilesWithoutDeletes_.toThrift());
    ret.setPath_hash_to_data_file_with_deletes(dataFilesWithDeletes_.toThrift());
    ret.setPath_hash_to_position_delete_file(positionDeleteFiles_.toThrift());
    ret.setPath_hash_to_equality_delete_file(equalityDeleteFiles_.toThrift());
    ret.setHas_avro(hasAvro_);
    ret.setHas_orc(hasOrc_);
    ret.setHas_parquet(hasParquet_);
    return ret;
  }

  public static IcebergContentFileStore fromThrift(TIcebergContentFileStore tFileStore,
      List<TNetworkAddress> networkAddresses,
      ListMap<TNetworkAddress> hostIndex) {
    IcebergContentFileStore ret = new IcebergContentFileStore();
    if (tFileStore.isSetPath_hash_to_data_file_without_deletes()) {
      ret.dataFilesWithoutDeletes_ = MapListContainer.fromThrift(
          tFileStore.getPath_hash_to_data_file_without_deletes(),
          networkAddresses, hostIndex);
    }
    if (tFileStore.isSetPath_hash_to_data_file_with_deletes()) {
      ret.dataFilesWithDeletes_ = MapListContainer.fromThrift(
          tFileStore.getPath_hash_to_data_file_with_deletes(),
          networkAddresses, hostIndex);
    }
    if (tFileStore.isSetPath_hash_to_position_delete_file()) {
      ret.positionDeleteFiles_ = MapListContainer.fromThrift(
          tFileStore.getPath_hash_to_position_delete_file(),
          networkAddresses, hostIndex);
    }
    if (tFileStore.isSetPath_hash_to_equality_delete_file()) {
      ret.equalityDeleteFiles_ = MapListContainer.fromThrift(
          tFileStore.getPath_hash_to_equality_delete_file(),
          networkAddresses, hostIndex);
    }
    ret.hasAvro_ = tFileStore.isSetHas_avro() ? tFileStore.isHas_avro() : false;
    ret.hasOrc_ = tFileStore.isSetHas_orc() ? tFileStore.isHas_orc() : false;
    ret.hasParquet_ = tFileStore.isSetHas_parquet() ? tFileStore.isHas_parquet() : false;
    return ret;
  }
}

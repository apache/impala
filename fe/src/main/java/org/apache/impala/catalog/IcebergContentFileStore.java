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
  // Key is the DataFile path hash, value is FileDescriptor transformed from DataFile
  private final Map<String, FileDescriptor> dataFileDescMap_ = new HashMap<>();
  private final Map<String, FileDescriptor> deleteFileDescMap_ = new HashMap<>();

  // List of Iceberg data files (doesn't include delete files)
  private final List<FileDescriptor> dataFiles_ = new ArrayList<>();

  // List of Iceberg delete files (equality and position delete files)
  private final List<FileDescriptor> deleteFiles_ = new ArrayList<>();

  // Caches file descriptors loaded during time-travel queries.
  private final ConcurrentMap<String, FileDescriptor> oldFileDescMap_ =
      new ConcurrentHashMap<>();

  // Flags to indicate file formats used in the table.
  private boolean hasAvro_ = false;
  private boolean hasOrc_ = false;
  private boolean hasParquet_ = false;

  public IcebergContentFileStore() {}

  public void addDataFileDescriptor(String pathHash, FileDescriptor desc) {
    if (dataFileDescMap_.put(pathHash, desc) == null) {
      dataFiles_.add(desc);
      updateFileFormats(desc);
    }
  }

  public void addDeleteFileDescriptor(String pathHash, FileDescriptor desc) {
    if (deleteFileDescMap_.put(pathHash, desc) == null) {
      deleteFiles_.add(desc);
      updateFileFormats(desc);
    }
  }

  public void addOldFileDescriptor(String pathHash, FileDescriptor desc) {
    oldFileDescMap_.put(pathHash, desc);
  }

  public FileDescriptor getDataFileDescriptor(String pathHash) {
    return dataFileDescMap_.get(pathHash);
  }

  public FileDescriptor getDeleteFileDescriptor(String pathHash) {
    return deleteFileDescMap_.get(pathHash);
  }

  public FileDescriptor getOldFileDescriptor(String pathHash) {
    return oldFileDescMap_.get(pathHash);
  }

  public FileDescriptor getFileDescriptor(String pathHash) {
    FileDescriptor desc = null;
    desc = dataFileDescMap_.get(pathHash);
    if (desc != null) return desc;
    desc = deleteFileDescMap_.get(pathHash);
    if (desc != null) return desc;
    desc = oldFileDescMap_.get(pathHash);
    return desc;
  }

  public List<FileDescriptor> getDataFiles() { return dataFiles_; }

  public List<FileDescriptor> getDeleteFiles() { return deleteFiles_; }

  public long getNumFiles() { return dataFiles_.size() + deleteFiles_.size(); }

  public Iterable<FileDescriptor> getAllFiles() {
    return Iterables.concat(dataFiles_, deleteFiles_);
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
    ret.setPath_hash_to_data_file(convertFileMapToThrift(dataFileDescMap_));
    ret.setPath_hash_to_delete_file(convertFileMapToThrift(deleteFileDescMap_));
    ret.setHas_avro(hasAvro_);
    ret.setHas_orc(hasOrc_);
    ret.setHas_parquet(hasParquet_);
    return ret;
  }

  public static IcebergContentFileStore fromThrift(TIcebergContentFileStore tFileStore,
      List<TNetworkAddress> networkAddresses,
      ListMap<TNetworkAddress> hostIndex) {
    IcebergContentFileStore ret = new IcebergContentFileStore();
    if (tFileStore.isSetPath_hash_to_data_file()) {
      convertFileMapFromThrift(tFileStore.getPath_hash_to_data_file(),
          ret.dataFileDescMap_, ret.dataFiles_, networkAddresses, hostIndex);
    }
    if (tFileStore.isSetPath_hash_to_delete_file()) {
      convertFileMapFromThrift(tFileStore.getPath_hash_to_delete_file(),
          ret.deleteFileDescMap_, ret.deleteFiles_, networkAddresses, hostIndex);
    }
    ret.hasAvro_ = tFileStore.isSetHas_avro() ? tFileStore.isHas_avro() : false;
    ret.hasOrc_ = tFileStore.isSetHas_orc() ? tFileStore.isHas_orc() : false;
    ret.hasParquet_ = tFileStore.isSetHas_parquet() ? tFileStore.isHas_parquet() : false;
    return ret;
  }

  private static Map<String, THdfsFileDesc> convertFileMapToThrift(
      Map<String, FileDescriptor> fileDescMap) {
    Map<String, THdfsFileDesc> ret = new HashMap<>();
    for (Map.Entry<String, HdfsPartition.FileDescriptor> entry : fileDescMap.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().toThrift());
    }
    return ret;
  }

  private static void convertFileMapFromThrift(Map<String, THdfsFileDesc> thriftMap,
      Map<String, FileDescriptor> outMap, List<FileDescriptor> outList,
      List<TNetworkAddress> networkAddresses, ListMap<TNetworkAddress> hostIndex) {
    Preconditions.checkNotNull(outMap);
    Preconditions.checkNotNull(outList);
    for (Map.Entry<String, THdfsFileDesc> entry : thriftMap.entrySet()) {
      FileDescriptor fd = FileDescriptor.fromThrift(entry.getValue());
      Preconditions.checkNotNull(fd);
      if (networkAddresses != null) {
        Preconditions.checkNotNull(hostIndex);
        fd = fd.cloneWithNewHostIndex(networkAddresses, hostIndex);
      }
      outMap.put(entry.getKey(), fd);
      outList.add(fd);
    }
  }
}

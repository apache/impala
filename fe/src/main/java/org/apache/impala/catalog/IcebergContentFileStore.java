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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.Pair;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.fb.FbFileMetadata;
import org.apache.impala.fb.FbIcebergDataFileFormat;
import org.apache.impala.fb.FbIcebergMetadata;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TIcebergContentFileStore;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for storing Iceberg file descriptors. It stores data and delete files
 * separately, while also storing file descriptors belonging to earlier snapshots.
 */
public class IcebergContentFileStore {
  final static Logger LOG = LoggerFactory.getLogger(IcebergContentFileStore.class);

  private static class EncodedFileDescriptor {
    public final byte[] fileDesc_;
    public final byte[] fileMetadata_;

    public EncodedFileDescriptor(byte[] fDesc, byte[] fMeta) {
      this.fileDesc_ = fDesc;
      this.fileMetadata_ = fMeta;
    }
  }

  protected static IcebergFileDescriptor decode(EncodedFileDescriptor encodedFd) {
    Preconditions.checkNotNull(encodedFd.fileDesc_);
    Preconditions.checkNotNull(encodedFd.fileMetadata_);

    return new IcebergFileDescriptor(
        FbFileDesc.getRootAsFbFileDesc(ByteBuffer.wrap(encodedFd.fileDesc_)),
        FbFileMetadata.getRootAsFbFileMetadata(ByteBuffer.wrap(encodedFd.fileMetadata_)));
  }

  // Function to convert from a FileDescriptor to an EncodedFileDescriptor.
  protected static EncodedFileDescriptor encode(IcebergFileDescriptor fd) {
    return new EncodedFileDescriptor(
        encodeFB(fd.getFbFileDescriptor()),
        encodeFB(fd.getFbFileMetadata()));
  }

  private static byte[] encodeFB(com.google.flatbuffers.Table fbObject) {
    if (fbObject == null) return null;

    ByteBuffer bb = fbObject.getByteBuffer();
    byte[] arr = bb.array();
    Preconditions.checkState(bb.arrayOffset() == 0 && bb.remaining() == arr.length);

    return arr;
  }

  // Auxiliary class for holding file descriptors in both a map and a list.
  private static class MapListContainer {
    // Key is the ContentFile path hash, value is FileDescriptor transformed from DataFile
    private final Map<String, EncodedFileDescriptor> fileDescMap_ = new HashMap<>();
    private final List<EncodedFileDescriptor> fileDescList_ = new ArrayList<>();

    // Adds a file to the map. If this is a new entry, then add it to the list as well.
    // Return true if 'desc' was a new entry.
    public boolean add(String pathHash, EncodedFileDescriptor desc) {
      if (fileDescMap_.put(pathHash, desc) == null) {
        fileDescList_.add(desc);
        return true;
      }
      return false;
    }

    public IcebergFileDescriptor get(String pathHash) {
      if (!fileDescMap_.containsKey(pathHash)) return null;
      return decode(fileDescMap_.get(pathHash));
    }

    public long getNumFiles() {
      return fileDescList_.size();
    }

    List<IcebergFileDescriptor> getList() {
      return Lists.transform(fileDescList_, fd -> IcebergContentFileStore.decode(fd));
    }

    // It's enough to only convert the map part to thrift.
    Map<String, THdfsFileDesc> toThrift() {
      Map<String, THdfsFileDesc> ret = new HashMap<>();
      for (Map.Entry<String, EncodedFileDescriptor> entry : fileDescMap_.entrySet()) {
        ret.put(
            entry.getKey(),
            decode(entry.getValue()).toThrift());
      }
      return ret;
    }

    static MapListContainer fromThrift(Map<String, THdfsFileDesc> thriftMap,
        List<TNetworkAddress> networkAddresses, ListMap<TNetworkAddress> hostIndex) {
      MapListContainer ret = new MapListContainer();
      for (Map.Entry<String, THdfsFileDesc> entry : thriftMap.entrySet()) {
        IcebergFileDescriptor fd = IcebergFileDescriptor.fromThrift(entry.getValue());
        Preconditions.checkNotNull(fd);
        if (networkAddresses != null) {
          Preconditions.checkNotNull(hostIndex);
          fd = fd.cloneWithNewHostIndex(networkAddresses, hostIndex);
        }
        ret.add(entry.getKey(), encode(fd));
      }
      return ret;
    }
  }

  // Separate map-list containers for the different content files.
  private MapListContainer dataFilesWithoutDeletes_ = new MapListContainer();
  private MapListContainer dataFilesWithDeletes_ = new MapListContainer();
  private MapListContainer positionDeleteFiles_ = new MapListContainer();
  private MapListContainer equalityDeleteFiles_ = new MapListContainer();
  private Set<String> missingFiles_ = new HashSet<>();

  // Caches file descriptors loaded during time-travel queries.
  private final ConcurrentMap<String, EncodedFileDescriptor> oldFileDescMap_ =
      new ConcurrentHashMap<>();

  // Flags to indicate file formats used in the table.
  private boolean hasAvro_ = false;
  private boolean hasOrc_ = false;
  private boolean hasParquet_ = false;

  public IcebergContentFileStore() {}

  public IcebergContentFileStore(
      Table iceApiTable, List<IcebergFileDescriptor> fileDescriptors,
      GroupedContentFiles icebergFiles) {
    Preconditions.checkNotNull(iceApiTable);
    Preconditions.checkNotNull(fileDescriptors);
    Preconditions.checkNotNull(icebergFiles);

    Map<String, IcebergFileDescriptor> fileDescMap = new HashMap<>();
    for (IcebergFileDescriptor fileDesc : fileDescriptors) {
      Path path = new Path(fileDesc.getAbsolutePath(iceApiTable.location()));
      fileDescMap.put(path.toUri().getPath(), fileDesc);
    }

    for (DataFile dataFile : icebergFiles.dataFilesWithoutDeletes) {
      storeFile(dataFile, fileDescMap, dataFilesWithoutDeletes_);
    }
    for (DataFile dataFile : icebergFiles.dataFilesWithDeletes) {
      storeFile(dataFile, fileDescMap, dataFilesWithDeletes_);
    }
    for (DeleteFile deleteFile : icebergFiles.positionDeleteFiles) {
      storeFile(deleteFile, fileDescMap, positionDeleteFiles_);
    }
    for (DeleteFile deleteFile : icebergFiles.equalityDeleteFiles) {
      storeFile(deleteFile, fileDescMap, equalityDeleteFiles_);
    }
  }

  private void storeFile(ContentFile<?> contentFile,
      Map<String, IcebergFileDescriptor> fileDescMap, MapListContainer container) {
    Pair<String, EncodedFileDescriptor> pathHashAndFd =
        getPathHashAndFd(contentFile, fileDescMap);
    if (pathHashAndFd.second != null) {
      container.add(pathHashAndFd.first, pathHashAndFd.second);
    } else {
      missingFiles_.add(contentFile.path().toString());
    }
  }

  // This is only invoked during time travel, when we are querying a snapshot that has
  // data files which have been removed since.
  public void addOldFileDescriptor(String pathHash, IcebergFileDescriptor desc) {
    oldFileDescMap_.put(pathHash, encode(desc));
  }

  public IcebergFileDescriptor getDataFileDescriptor(String pathHash) {
    IcebergFileDescriptor desc = dataFilesWithoutDeletes_.get(pathHash);
    if (desc != null) return desc;
    return dataFilesWithDeletes_.get(pathHash);
  }

  public IcebergFileDescriptor getDeleteFileDescriptor(String pathHash) {
    IcebergFileDescriptor ret = positionDeleteFiles_.get(pathHash);
    if (ret != null) return ret;
    return equalityDeleteFiles_.get(pathHash);
  }

  public IcebergFileDescriptor getOldFileDescriptor(String pathHash) {
    if (!oldFileDescMap_.containsKey(pathHash)) return null;
    return decode(oldFileDescMap_.get(pathHash));
  }

  public List<IcebergFileDescriptor> getDataFilesWithoutDeletes() {
    return dataFilesWithoutDeletes_.getList();
  }

  public List<IcebergFileDescriptor> getDataFilesWithDeletes() {
    return dataFilesWithDeletes_.getList();
  }

  public List<IcebergFileDescriptor> getPositionDeleteFiles() {
    return positionDeleteFiles_.getList();
  }

  public List<IcebergFileDescriptor> getEqualityDeleteFiles() {
    return equalityDeleteFiles_.getList();
  }

  public boolean hasMissingFile() {
    return !missingFiles_.isEmpty();
  }

  public Set<String> getMissingFiles() {
    return missingFiles_;
  }

  public long getNumFiles() {
    return dataFilesWithoutDeletes_.getNumFiles() +
           dataFilesWithDeletes_.getNumFiles() +
           positionDeleteFiles_.getNumFiles() +
           equalityDeleteFiles_.getNumFiles();
  }

  public Iterable<IcebergFileDescriptor> getAllFiles() {
    return Iterables.concat(
        dataFilesWithoutDeletes_.getList(),
        dataFilesWithDeletes_.getList(),
        positionDeleteFiles_.getList(),
        equalityDeleteFiles_.getList());
  }

  public Iterable<IcebergFileDescriptor> getAllDataFiles() {
    return Iterables.concat(
        dataFilesWithoutDeletes_.getList(),
        dataFilesWithDeletes_.getList());
  }

  public boolean hasAvro() { return hasAvro_; }
  public boolean hasOrc() { return hasOrc_; }
  public boolean hasParquet() { return hasParquet_; }

  private void updateFileFormats(FbIcebergMetadata icebergMetadata) {
    Preconditions.checkNotNull(icebergMetadata);

    byte fileFormat = icebergMetadata.fileFormat();
    if (fileFormat == FbIcebergDataFileFormat.PARQUET) {
      hasParquet_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.ORC) {
      hasOrc_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.AVRO) {
      hasAvro_ = true;
    }
  }

  private Pair<String, EncodedFileDescriptor> getPathHashAndFd(
      ContentFile<?> contentFile, Map<String, IcebergFileDescriptor> fileDescMap) {
    return new Pair<>(
        IcebergUtil.getFilePathHash(contentFile),
        getIcebergFd(fileDescMap, contentFile));
  }

  private EncodedFileDescriptor getIcebergFd(
      Map<String, IcebergFileDescriptor> fileDescMap,
      ContentFile<?> contentFile) {
    Path path = new Path(contentFile.path().toString());
    IcebergFileDescriptor fileDesc = fileDescMap.get(path.toUri().getPath());

    if (fileDesc == null) return null;

    FbFileMetadata fileMetadata = fileDesc.getFbFileMetadata();
    Preconditions.checkState(fileMetadata != null);
    FbIcebergMetadata icebergMetadata = fileMetadata.icebergMetadata();
    Preconditions.checkState(icebergMetadata != null);

    updateFileFormats(icebergMetadata);

    return new EncodedFileDescriptor(
        encodeFB(fileDesc.getFbFileDescriptor()),
        encodeFB(icebergMetadata));
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
    ret.setMissing_files(new ArrayList<>(missingFiles_));
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
    ret.missingFiles_ = tFileStore.isSetMissing_files() ?
        new HashSet<>(tFileStore.getMissing_files()) : Collections.emptySet();
    return ret;
  }
}

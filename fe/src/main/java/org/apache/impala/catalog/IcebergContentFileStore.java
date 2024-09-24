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
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.Pair;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.fb.FbFileMetadata;
import org.apache.impala.fb.FbIcebergDataFileFormat;
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

  protected static FileDescriptor decode(EncodedFileDescriptor encodedFd) {
    Preconditions.checkNotNull(encodedFd.fileMetadata_);

    return new FileDescriptor(
        FbFileDesc.getRootAsFbFileDesc(ByteBuffer.wrap(encodedFd.fileDesc_)),
        FbFileMetadata.getRootAsFbFileMetadata(
            ByteBuffer.wrap(encodedFd.fileMetadata_))) {
      // Whenever getting the FileDescriptor for the same path hash it will be a
      // different FileDescriptor object. As a result the default equals() and
      // hashCode() functions can't be used to judge equality between these
      // FileDescriptors.
      @Override
      public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof FileDescriptor)) return false;
        FileDescriptor otherFD = (FileDescriptor) obj;
        if ((this.getFbFileMetadata() == null && otherFD.getFbFileMetadata() != null) ||
            (this.getFbFileMetadata() != null && otherFD.getFbFileMetadata() == null)) {
          return false;
        }
        return this.getFbFileDescriptor().getByteBuffer().array() ==
            otherFD.getFbFileDescriptor().getByteBuffer().array() &&
            this.getFbFileMetadata().getByteBuffer().array() ==
                otherFD.getFbFileMetadata().getByteBuffer().array();
      }

      @Override
      public int hashCode() {
        return getAbsolutePath().hashCode();
      }
    };
  }

  // Function to convert from a FileDescriptor to an EncodedFileDescriptor.
  protected static EncodedFileDescriptor encode(FileDescriptor fd) {
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

    public FileDescriptor get(String pathHash) {
      if (!fileDescMap_.containsKey(pathHash)) return null;
      return decode(fileDescMap_.get(pathHash));
    }

    public long getNumFiles() {
      return fileDescList_.size();
    }

    List<FileDescriptor> getList() {
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
        FileDescriptor fd = FileDescriptor.fromThrift(entry.getValue());
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

  // Caches file descriptors loaded during time-travel queries.
  private final ConcurrentMap<String, EncodedFileDescriptor> oldFileDescMap_ =
      new ConcurrentHashMap<>();

  // Flags to indicate file formats used in the table.
  private boolean hasAvro_ = false;
  private boolean hasOrc_ = false;
  private boolean hasParquet_ = false;

  public IcebergContentFileStore() {}

  public IcebergContentFileStore(
      FeIcebergTable icebergTable, GroupedContentFiles icebergFiles) {
    Preconditions.checkNotNull(icebergTable);
    Preconditions.checkNotNull(icebergFiles);

    Map<String, FileDescriptor> hdfsFileDescMap = new HashMap<>();
    Collection<? extends FeFsPartition> partitions =
        FeCatalogUtils.loadAllPartitions(icebergTable.getFeFsTable());
    for (FeFsPartition partition : partitions) {
      for (FileDescriptor fileDesc : partition.getFileDescriptors()) {
        Path path = new Path(fileDesc.getAbsolutePath(icebergTable.getHdfsBaseDir()));
        hdfsFileDescMap.put(path.toUri().getPath(), fileDesc);
      }
    }

    for (DataFile dataFile : icebergFiles.dataFilesWithoutDeletes) {
      Pair<String, EncodedFileDescriptor> pathHashAndFd =
          getPathHashAndFd(icebergTable, dataFile, hdfsFileDescMap);
      dataFilesWithoutDeletes_.add(pathHashAndFd.first, pathHashAndFd.second);
    }
    for (DataFile dataFile : icebergFiles.dataFilesWithDeletes) {
      Pair<String, EncodedFileDescriptor> pathHashAndFd =
          getPathHashAndFd(icebergTable, dataFile, hdfsFileDescMap);
      dataFilesWithDeletes_.add(pathHashAndFd.first, pathHashAndFd.second);
    }
    for (DeleteFile deleteFile : icebergFiles.positionDeleteFiles) {
      Pair<String, EncodedFileDescriptor> pathHashAndFd =
          getPathHashAndFd(icebergTable, deleteFile, hdfsFileDescMap);
      positionDeleteFiles_.add(pathHashAndFd.first, pathHashAndFd.second);
    }
    for (DeleteFile deleteFile : icebergFiles.equalityDeleteFiles) {
      Pair<String, EncodedFileDescriptor> pathHashAndFd =
          getPathHashAndFd(icebergTable, deleteFile, hdfsFileDescMap);
      equalityDeleteFiles_.add(pathHashAndFd.first, pathHashAndFd.second);
    }
  }

  // This is only invoked during time travel, when we are querying a snapshot that has
  // data files which have been removed since.
  public void addOldFileDescriptor(String pathHash, FileDescriptor desc) {
    oldFileDescMap_.put(pathHash, encode(desc));
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
    if (!oldFileDescMap_.containsKey(pathHash)) return null;
    return decode(oldFileDescMap_.get(pathHash));
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

  private void updateFileFormats(FbFileMetadata icebergMetadata) {
    Preconditions.checkNotNull(icebergMetadata);
    Preconditions.checkNotNull(icebergMetadata.icebergMetadata());

    byte fileFormat = icebergMetadata.icebergMetadata().fileFormat();
    if (fileFormat == FbIcebergDataFileFormat.PARQUET) {
      hasParquet_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.ORC) {
      hasOrc_ = true;
    } else if (fileFormat == FbIcebergDataFileFormat.AVRO) {
      hasAvro_ = true;
    }
  }

  private Pair<String, EncodedFileDescriptor> getPathHashAndFd(
      FeIcebergTable icebergTable,
      ContentFile<?> contentFile,
      Map<String, FileDescriptor> hdfsFileDescMap) {
    return new Pair<>(
        IcebergUtil.getFilePathHash(contentFile),
        getOrCreateIcebergFd(icebergTable, hdfsFileDescMap, contentFile));
  }

  private EncodedFileDescriptor getOrCreateIcebergFd(
      FeIcebergTable icebergTable,
      Map<String, FileDescriptor> hdfsFileDescMap,
      ContentFile<?> contentFile) {
    Path path = new Path(contentFile.path().toString());
    FileDescriptor fileDesc = null;
    if (hdfsFileDescMap.containsKey(path.toUri().getPath())) {
      fileDesc = hdfsFileDescMap.get(path.toUri().getPath());
    } else {
      if (FeIcebergTable.Utils.requiresDataFilesInTableLocation(icebergTable)) {
        LOG.warn("Iceberg file '{}' cannot be found in the HDFS recursive"
            + "file listing results.", path);
      }
      try {
        fileDesc = FeIcebergTable.Utils.getFileDescriptor(contentFile, icebergTable);
      } catch (IOException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    Preconditions.checkNotNull(fileDesc);

    FbFileMetadata icebergMetadata =
        IcebergUtil.createIcebergMetadata(icebergTable, contentFile);

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
    return ret;
  }

  // TODO IMPALA-11265: After converting to/from thrift the byte arrays representing the
  // file descriptors won't be shared between the HdfsTable and IcebergContentFileStore.
  // This redundancy causes unnecessary JVM heap memory usage on the coordinator.
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

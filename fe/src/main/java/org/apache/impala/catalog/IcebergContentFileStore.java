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
import java.lang.ref.SoftReference;
import java.util.stream.Collectors;

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
import org.apache.impala.thrift.THash128;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TIcebergContentFileStore;
import org.apache.impala.thrift.TIcebergDeletionVector;
import org.apache.impala.thrift.TIcebergPartition;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.Hash128;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for storing Iceberg file descriptors. It stores data and delete files
 * separately, while also storing file descriptors belonging to earlier snapshots.
 * Shared between queries on the Coordinator side.
 */
public class IcebergContentFileStore {

  private final static Logger LOG = LoggerFactory.getLogger(
      IcebergContentFileStore.class);

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
    private final Map<Hash128, EncodedFileDescriptor> fileDescMap_ = new HashMap<>();
    private final List<EncodedFileDescriptor> fileDescList_ = new ArrayList<>();
    // Reverse map built lazily on first partial serialization and reused across pages.
    // Held via SoftReference so the JVM can reclaim it under memory pressure.
    private SoftReference<Map<EncodedFileDescriptor, Hash128>> reverseMap_;

    // Adds a file to the map. If this is a new entry, then add it to the list as well.
    // Return true if 'desc' was a new entry.
    public boolean add(Hash128 pathHash, EncodedFileDescriptor desc) {
      if (fileDescMap_.put(pathHash, desc) == null) {
        fileDescList_.add(desc);
        return true;
      }
      return false;
    }

    public IcebergFileDescriptor get(Hash128 pathHash) {
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
    Map<THash128, THdfsFileDesc> toThrift() {
      Map<THash128, THdfsFileDesc> ret = new HashMap<>();
      for (Map.Entry<Hash128, EncodedFileDescriptor> entry : fileDescMap_.entrySet()) {
        ret.put(
            entry.getKey().toThrift(),
            decode(entry.getValue()).toThrift());
      }
      return ret;
    }

    /**
     * Convert a range of files to thrift for partial RPC response.
     *
     * @param startOffset Index to start from (0-based)
     * @param maxFiles Maximum files to include
     * @return Thrift map for the requested range
     */
    Map<THash128, THdfsFileDesc> toThriftPartial(long startOffset, long maxFiles) {
      if (startOffset >= fileDescList_.size()) return Collections.emptyMap();

      long endOffset = Math.min(startOffset + maxFiles, fileDescList_.size());

      // Fast path: if requesting the entire container, use toThrift() directly
      if (startOffset == 0 && endOffset == fileDescList_.size()) return toThrift();

      Map<EncodedFileDescriptor, Hash128> reverseMap =
          (reverseMap_ != null) ? reverseMap_.get() : null;
      if (reverseMap == null) {
        reverseMap = fileDescMap_.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        reverseMap_ = new SoftReference<>(reverseMap);
      }

      Map<THash128, THdfsFileDesc> ret = new HashMap<>();
      for (int i = (int)startOffset; i < endOffset; i++) {
        EncodedFileDescriptor encodedFd = fileDescList_.get(i);
        Hash128 pathHash = reverseMap.get(encodedFd);
        Preconditions.checkNotNull(pathHash, "FileDescriptor in list but not in map");
        ret.put(pathHash.toThrift(), decode(encodedFd).toThrift());
      }
      return ret;
    }

    static MapListContainer fromThrift(Map<THash128, THdfsFileDesc> thriftMap,
        List<TNetworkAddress> networkAddresses, ListMap<TNetworkAddress> hostIndex) {
      MapListContainer ret = new MapListContainer();
      for (Map.Entry<THash128, THdfsFileDesc> entry : thriftMap.entrySet()) {
        IcebergFileDescriptor fd = IcebergFileDescriptor.fromThrift(entry.getValue());
        Preconditions.checkNotNull(fd);
        if (networkAddresses != null) {
          Preconditions.checkNotNull(hostIndex);
          fd = fd.cloneWithNewHostIndex(networkAddresses, hostIndex);
        }
        ret.add(Hash128.fromThrift(entry.getKey()), encode(fd));
      }
      return ret;
    }
  }

  // Separate map-list containers for the different content files.
  private MapListContainer dataFilesWithoutDeletes_ = new MapListContainer();
  private MapListContainer dataFilesWithDeletes_ = new MapListContainer();
  private MapListContainer positionDeleteFiles_ = new MapListContainer();
  private MapListContainer equalityDeleteFiles_ = new MapListContainer();
  private Map<Hash128, TIcebergDeletionVector> dataFileToDV_ = new HashMap<>();
  private Set<String> missingFiles_ = new HashSet<>();
  // Partitions with their corresponding ids that are used to refer to the partition info
  // from the IcebergFileDescriptors.
  private Map<TIcebergPartition, Integer> partitions_;

  // Caches file descriptors loaded during time-travel queries.
  private final ConcurrentMap<Hash128, EncodedFileDescriptor> oldFileDescMap_ =
      new ConcurrentHashMap<>();
  // Caches the partitions of file descriptors loaded during time-travel queries.
  private final ConcurrentMap<TIcebergPartition, Integer> oldPartitionMap_ =
      new ConcurrentHashMap<>();

  // Flags to indicate file formats used in the table.
  private boolean hasAvro_ = false;
  private boolean hasOrc_ = false;
  private boolean hasParquet_ = false;

  public IcebergContentFileStore() {}

  public IcebergContentFileStore(
      Table iceApiTable, List<IcebergFileDescriptor> fileDescriptors,
      GroupedContentFiles icebergFiles, Map<TIcebergPartition, Integer> partitions) {
    Preconditions.checkNotNull(iceApiTable);
    Preconditions.checkNotNull(fileDescriptors);
    Preconditions.checkNotNull(icebergFiles);
    Preconditions.checkNotNull(partitions);

    partitions_ = partitions;

    Map<String, IcebergFileDescriptor> fileDescMap = new HashMap<>();
    String apiTableLocation = iceApiTable.location();
    for (IcebergFileDescriptor fileDesc : fileDescriptors) {
      String absPathStr = fileDesc.getAbsolutePath(apiTableLocation);
      String pathStr = quickGetPath(absPathStr);
      fileDescMap.put(pathStr, fileDesc);
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
    dataFileToDV_ = icebergFiles.dataFileToDV;
  }

  private void storeFile(ContentFile<?> contentFile,
      Map<String, IcebergFileDescriptor> fileDescMap, MapListContainer container) {
    Pair<Hash128, EncodedFileDescriptor> pathHashAndFd =
        getPathHashAndFd(contentFile, fileDescMap);
    if (pathHashAndFd.second != null) {
      container.add(pathHashAndFd.first, pathHashAndFd.second);
    } else {
      missingFiles_.add(contentFile.path().toString());
    }
  }

  // This is only invoked during time travel, when we are querying a snapshot that has
  // data files which have been removed since.
  public void addOldFileDescriptor(Hash128 pathHash, IcebergFileDescriptor desc) {
    oldFileDescMap_.put(pathHash, encode(desc));
  }

  // This is only invoked during time travel, when we are querying a snapshot that has
  // partitions which have been removed since.
  public void addOldPartition(TIcebergPartition partition, Integer id) {
    oldPartitionMap_.put(partition, id);
  }

  public IcebergFileDescriptor getDataFileDescriptor(Hash128 pathHash) {
    IcebergFileDescriptor desc = dataFilesWithoutDeletes_.get(pathHash);
    if (desc != null) return desc;
    return dataFilesWithDeletes_.get(pathHash);
  }

  public IcebergFileDescriptor getDeleteFileDescriptor(Hash128 pathHash) {
    IcebergFileDescriptor ret = positionDeleteFiles_.get(pathHash);
    if (ret != null) return ret;
    return equalityDeleteFiles_.get(pathHash);
  }

  public IcebergFileDescriptor getOldFileDescriptor(Hash128 pathHash) {
    if (!oldFileDescMap_.containsKey(pathHash)) return null;
    return decode(oldFileDescMap_.get(pathHash));
  }

  public Integer getOldPartition(TIcebergPartition partition) {
    return oldPartitionMap_.get(partition);
  }

  public int getOldPartitionsSize() {
    return oldPartitionMap_.size();
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

  public Map<Hash128, TIcebergDeletionVector> getDataFileToDV() {
    return dataFileToDV_;
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

  public Iterable<IcebergFileDescriptor> getAllDeleteFiles() {
    return Iterables.concat(
        positionDeleteFiles_.getList(),
        equalityDeleteFiles_.getList());
  }

  public Map<TIcebergPartition, Integer> getPartitionMap() {
    return partitions_;
  }

  public List<TIcebergPartition> getPartitionList() {
    return convertPartitionMapToList(partitions_);
  }

  public int getNumPartitions() {
    return partitions_.size();
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

  private Pair<Hash128, EncodedFileDescriptor> getPathHashAndFd(
      ContentFile<?> contentFile, Map<String, IcebergFileDescriptor> fileDescMap) {
    return new Pair<>(
        IcebergUtil.getFilePathHash(contentFile),
        getIcebergFd(fileDescMap, contentFile));
  }

  private static String quickGetPath(String uri) {
    int pos1 = uri.indexOf('/');
    int pos2 = uri.indexOf('/', pos1 + 1);
    if (pos2 != pos1 + 1) {
      // Assume no scheme and authority, return whole path.
      return uri;
    }
    int pos3 = uri.indexOf('/', pos2 + 1);

    if (pos3 != -1) {
        // Return everything starting from the 3rd slash.
        return uri.substring(pos3);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("quickGetPath couldn't parse uri, falling back to slow path: {}", uri);
    }
    // Something is off, use slow path.
    return new Path(uri).toUri().getPath();
  }

  private EncodedFileDescriptor getIcebergFd(
      Map<String, IcebergFileDescriptor> fileDescMap,
      ContentFile<?> contentFile) {
    String pathStr = quickGetPath(contentFile.location());
    IcebergFileDescriptor fileDesc = fileDescMap.get(pathStr);

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
    Map<THash128, THdfsFileDesc> m;
    m = dataFilesWithoutDeletes_.toThrift();
    if (!m.isEmpty()) ret.setPath_hash_to_data_file_without_deletes(m);
    m = dataFilesWithDeletes_.toThrift();
    if (!m.isEmpty()) ret.setPath_hash_to_data_file_with_deletes(m);
    m = positionDeleteFiles_.toThrift();
    if (!m.isEmpty()) ret.setPath_hash_to_position_delete_file(m);
    m = equalityDeleteFiles_.toThrift();
    if (!m.isEmpty()) ret.setPath_hash_to_equality_delete_file(m);
    ret.setHas_avro(hasAvro_);
    ret.setHas_orc(hasOrc_);
    ret.setHas_parquet(hasParquet_);
    ret.setMissing_files(new ArrayList<>(missingFiles_));
    ret.setPartitions(convertPartitionMapToList(partitions_));
    Map<THash128, TIcebergDeletionVector> tdeletion_vectors = new HashMap<>();
    for (Map.Entry<Hash128, TIcebergDeletionVector> entry : dataFileToDV_.entrySet()) {
      tdeletion_vectors.put(entry.getKey().toThrift(), entry.getValue());
    }
    ret.setData_path_hash_to_dv(tdeletion_vectors);
    return ret;
  }

  /**
   * Helper class to track state during partial serialization
   */
  private static class PartialSerializationState {
    long currentOffset = 0;
    long filesCollected = 0;
  }

  /**
   * Process a single container for partial serialization
   */
  private void processContainerPartial(
      MapListContainer container,
      long startOffset,
      long maxFiles,
      PartialSerializationState state,
      java.util.function.Consumer<Map<THash128, THdfsFileDesc>> setter) {
    long containerSize = container.getNumFiles();
    if (startOffset < state.currentOffset + containerSize &&
        state.filesCollected < maxFiles) {
      long localOffset = Math.max(0, startOffset - state.currentOffset);
      Map<THash128, THdfsFileDesc> result =
          container.toThriftPartial(localOffset, maxFiles - state.filesCollected);
      if (!result.isEmpty()) {
        setter.accept(result);
        state.filesCollected += result.size();
      }
    }
    state.currentOffset += containerSize;
  }

  /**
   * Convert to thrift representation with file range limiting for partial RPC.
   * Files are paginated across all four file type collections.
   *
   * @param startOffset Global offset to start from (across all file types)
   * @param maxFiles Maximum files to include in response
   * @return Partial TIcebergContentFileStore with subset of files
   */
  public TIcebergContentFileStore toThriftPartial(long startOffset, long maxFiles) {
    TIcebergContentFileStore ret = new TIcebergContentFileStore();
    PartialSerializationState state = new PartialSerializationState();

    processContainerPartial(dataFilesWithoutDeletes_, startOffset, maxFiles, state,
        ret::setPath_hash_to_data_file_without_deletes);
    processContainerPartial(dataFilesWithDeletes_, startOffset, maxFiles, state,
        ret::setPath_hash_to_data_file_with_deletes);
    processContainerPartial(positionDeleteFiles_, startOffset, maxFiles, state,
        ret::setPath_hash_to_position_delete_file);
    processContainerPartial(equalityDeleteFiles_, startOffset, maxFiles, state,
        ret::setPath_hash_to_equality_delete_file);

    // Only include metadata in first request to reduce response size
    if (startOffset == 0) {
      ret.setHas_avro(hasAvro_);
      ret.setHas_orc(hasOrc_);
      ret.setHas_parquet(hasParquet_);
      ret.setMissing_files(new ArrayList<>(missingFiles_));
      ret.setPartitions(convertPartitionMapToList(partitions_));
      Map<THash128, TIcebergDeletionVector> tdeletion_vectors = new HashMap<>();
      for (Map.Entry<Hash128, TIcebergDeletionVector> entry : dataFileToDV_.entrySet()) {
        tdeletion_vectors.put(entry.getKey().toThrift(), entry.getValue());
      }
      ret.setData_path_hash_to_dv(tdeletion_vectors);
    }

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
    if (tFileStore.isSetData_path_hash_to_dv()) {
      for (Map.Entry<THash128, TIcebergDeletionVector> entry :
          tFileStore.getData_path_hash_to_dv().entrySet()) {
        ret.dataFileToDV_.put(Hash128.fromThrift(entry.getKey()), entry.getValue());
      }
    }
    ret.hasAvro_ = tFileStore.isSetHas_avro() ? tFileStore.isHas_avro() : false;
    ret.hasOrc_ = tFileStore.isSetHas_orc() ? tFileStore.isHas_orc() : false;
    ret.hasParquet_ = tFileStore.isSetHas_parquet() ? tFileStore.isHas_parquet() : false;
    ret.missingFiles_ = tFileStore.isSetMissing_files() ?
        new HashSet<>(tFileStore.getMissing_files()) : Collections.emptySet();
    ret.partitions_ = tFileStore.isSetPartitions() ?
        convertPartitionListToMap(tFileStore.getPartitions()) : new HashMap<>();
    return ret;
  }

  static List<TIcebergPartition> convertPartitionMapToList(
      Map<TIcebergPartition, Integer> partitionMap) {
    List<TIcebergPartition> partitionList = new ArrayList<>(partitionMap.size());
    for (int i = 0; i < partitionMap.size(); i++) {
      partitionList.add(null);
    }
    for (Map.Entry<TIcebergPartition, Integer> partition : partitionMap.entrySet()) {
      partitionList.set(partition.getValue(), partition.getKey());
    }
    return partitionList;
  }

  private static ImmutableMap<TIcebergPartition, Integer> convertPartitionListToMap(
      List<TIcebergPartition> partitionList) {
    Preconditions.checkState(partitionList != null);
    ImmutableMap.Builder<TIcebergPartition, Integer> builder = ImmutableMap.builder();
    for (int i = 0; i < partitionList.size(); ++i) {
      builder.put(partitionList.get(i), i);
    }
    return builder.build();
  }

  /**
   * Count total files in a TIcebergContentFileStore.
   */
  public static long countContentFiles(TIcebergContentFileStore contentFiles) {
    if (contentFiles == null) return 0;

    return contentFiles.getPath_hash_to_data_file_without_deletesSize()
        + contentFiles.getPath_hash_to_data_file_with_deletesSize()
        + contentFiles.getPath_hash_to_position_delete_fileSize()
        + contentFiles.getPath_hash_to_equality_delete_fileSize();
  }

  /**
   * Merge file maps from nextContentFiles into baseContentFiles.
   */
  public static void mergeContentFiles(
      TIcebergContentFileStore baseContentFiles,
      TIcebergContentFileStore nextContentFiles) {
    if (nextContentFiles.isSetPath_hash_to_data_file_without_deletes()) {
      if (!baseContentFiles.isSetPath_hash_to_data_file_without_deletes()) {
        baseContentFiles.setPath_hash_to_data_file_without_deletes(new HashMap<>());
      }
      baseContentFiles.path_hash_to_data_file_without_deletes.putAll(
          nextContentFiles.path_hash_to_data_file_without_deletes);
    }

    if (nextContentFiles.isSetPath_hash_to_data_file_with_deletes()) {
      if (!baseContentFiles.isSetPath_hash_to_data_file_with_deletes()) {
        baseContentFiles.setPath_hash_to_data_file_with_deletes(new HashMap<>());
      }
      baseContentFiles.path_hash_to_data_file_with_deletes.putAll(
          nextContentFiles.path_hash_to_data_file_with_deletes);
    }

    if (nextContentFiles.isSetPath_hash_to_position_delete_file()) {
      if (!baseContentFiles.isSetPath_hash_to_position_delete_file()) {
        baseContentFiles.setPath_hash_to_position_delete_file(new HashMap<>());
      }
      baseContentFiles.path_hash_to_position_delete_file.putAll(
          nextContentFiles.path_hash_to_position_delete_file);
    }

    if (nextContentFiles.isSetPath_hash_to_equality_delete_file()) {
      if (!baseContentFiles.isSetPath_hash_to_equality_delete_file()) {
        baseContentFiles.setPath_hash_to_equality_delete_file(new HashMap<>());
      }
      baseContentFiles.path_hash_to_equality_delete_file.putAll(
          nextContentFiles.path_hash_to_equality_delete_file);
    }

    if (nextContentFiles.isSetData_path_hash_to_dv()) {
      if (!baseContentFiles.isSetData_path_hash_to_dv()) {
        baseContentFiles.setData_path_hash_to_dv(new HashMap<>());
      }
      baseContentFiles.data_path_hash_to_dv.putAll(
          nextContentFiles.data_path_hash_to_dv);
    }
  }
}

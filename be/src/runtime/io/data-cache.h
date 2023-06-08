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

#pragma once

#include <mutex>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <gtest/gtest_prod.h>
#include <boost/thread/pthread/shared_mutex.hpp>

#include "common/status.h"
#include "util/cache/cache.h"
#include "util/metrics-fwd.h"
#include "util/spinlock.h"
#include "util/thread-pool.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

/// This class is an implementation of an IO data cache which is backed by local storage.
/// It implicitly relies on the OS page cache management to shuffle data between memory
/// and the storage device. This is useful for caching data read from remote filesystems
/// (e.g. remote HDFS data node, S3, ABFS, ADLS).
///
/// A data cache is divided into one or more partitions based on the configuration
/// string which specifies a list of directories and their corresponding storage quotas.
///
/// Each partition has a meta-data cache which tracks the mappings of cache keys to
/// the locations of the cached data. A cache key is a tuple of (file's name, file's
/// modification time, file offset) and a cache entry is a tuple of (backing file,
/// offset in the backing file, length of the cached data, optional checksum). The
/// file's modification time is used for distinguishing between different versions of
/// a file with a given name. Each partition stores its set of cached data in backing
/// files created on local storage. When inserting new data into the cache, the data is
/// appended to the current backing file in use. The storage consumption of each cache
/// entry counts towards the quota of that partition. When a partition reaches its
/// capacity, the least recently used data in that partition is evicted. Evicted data is
/// removed from the underlying storage by punching holes in the backing file it's stored
/// in. As a backing file reaches a certain size (e.g. 4TB), new data will stop being
/// appended to it and a new file will be created instead. Note that due to hole punching,
/// the backing file is actually sparse. For instance, a backing file may look like the
/// following after some insertion and eviction. All the holes in file consume no storage
/// space at all.
///
/// 0                                                                             1GB
/// +----------+----------+----------+-----------------+---------+---------+-------+
/// |          |          |          |                 |         |         |       |
/// |   Data   |   Hole   |   Data   |       Hole      |  Data   |  Hole   |  Data |
/// |          |          |          |                 |         |         |       |
/// +----------+----------+----------+-----------------+---------+---------+-------+
///                                                                                ^
///                                                                    insertion offset
///
/// Optionally, checksumming can be enabled to verify read from the cache is consistent
/// with what was inserted and to verify that multiple attempted insertions with the same
/// cache key have the same cache content.
///
/// Note that the cache currently doesn't support sub-ranges lookup and or handle
/// overlapping ranges. In other words, if the cache has an entry for a file at range
/// [0,4095], a look up for range [4000,4095] will result in a miss even though it's a
/// sub-range of [0,4095]. Also inserting the range [4000,4095] will not consolidate
/// with any overlapping ranges. In other words, inserting entries for ranges [0,4095]
/// and [4000,4095] will result in caching the data for range [4000,4095] twice. This
/// hasn't been a major concern in practice when testing with TPC-DS + parquet but this
/// requires more investigation for other file formats and workloads.
///
/// To probe for cached data in the cache, the interface Lookup() is used; To insert
/// data into the cache, the interface Store() is used. Write to the backing file and
/// eviction from it happen synchronously. Currently, Store() is limited to the
/// concurrency of one thread per partition to prevent slowing down the caller in case
/// the cache is thrashing and it becomes IO bound. The write concurrency can be tuned
/// via the knob --data_cache_write_concurrency. Also, Store() has a minimum granularity
/// of 4KB so any data inserted will be rounded up to the nearest multiple of 4KB.
///
/// The number of backing files in all partitions is bound by
/// --data_cache_max_opened_files. Once the number of files exceeds that set limit, files
/// are closed and deleted asynchronously by thread in 'file_deleter_pool_'. Stale cache
/// entries which reference deleted files are erased lazily upon the next access or
/// indirectly via eviction.
///
/// Future work:
/// - investigate the overlapping ranges support
/// - be more selective on what to cache
/// - asynchronous eviction
/// - better data placement: put on hot data on faster media and lukewarm data in not
///   so fast storage media
/// - evaluate the option of exposing the cache via mmap() and pinning similar to HDFS
///   caching. This has the advantage of not needing to copy out the data but pinning
///   may complicate the code.
///

namespace impala {
namespace io {

namespace trace {
  class Tracer;
  enum class EventType;
}

class DataCache {
 public:

  /// 'config' is the configuration string of the form <dir1>,...,<dirN>:<quota>
  /// in which <dir1>, <dirN> are part of a list of directories for storing cached data
  /// and each directory corresponds to a cache partition. <quota> is the storage quota
  /// for each directory. Impala daemons running on the same host will not share any
  /// caching directories. If 'trace_replay' is set to true, the cache operates in a
  /// an optimized mode that skips all file operations and only does the metadata
  /// operations. This is used to replay the access trace and compare different cache
  /// configurations. See data-cache-trace.h
  explicit DataCache(const std::string config, int32_t num_async_write_threads = 0,
      bool trace_replay = false);

  ~DataCache();

  /// Parses the configuration string, initializes all partitions in the cache by
  /// checking for storage space available and creates a backing file for caching.
  /// Return error if any of the partitions failed to be initialized.
  Status Init();

  /// Releases any resources (e.g. backing files) consumed by all partitions.
  void ReleaseResources();

  /// Looks up a cached entry and copies any cached content from the cache into 'buffer'.
  /// (filename, mtime, offset) forms a cache key. Please note that sub-range lookup is
  /// currently not supported. See header comments for details.
  ///
  /// 'filename'      : name of the requested file
  /// 'mtime'         : the modification time of the requested file
  /// 'offset'        : starting offset of the requested region in the file
  /// 'bytes_to_read' : number of bytes to be read from the cache
  /// 'buffer'        : output buffer to be written into on cache hit or nullptr for
  ///                   trace replay
  ///
  /// Returns the number of bytes read from the cache on cache hit; Returns 0 otherwise.
  ///
  int64_t Lookup(const std::string& filename, int64_t mtime, int64_t offset,
      int64_t bytes_to_read, uint8_t* buffer);

  /// Inserts a new cache entry by copying the content in 'buffer' into the cache.
  /// (filename, mtime, offset) together forms a cache key. Insertion involves writing
  /// to the backing file and potentially evicting entries synchronously so callers
  /// may want to avoid holding locks while calling this function.
  ///
  /// 'filename'      : name of the file being inserted
  /// 'mtime'         : the modification time of the file being inserted.
  /// 'offset'        : the starting offset of the region in the file being inserted
  /// 'buffer'        : buffer holding the data to be inserted or nullptr for trace
  ///                   replay
  /// 'buffer_len'    : size of 'buffer'
  ///
  /// The cache key is hashed and the resulting hash determines the partition to use.
  ///
  /// Please note that 'buffer_len' is rounded up to the nearest multiple of 4KB when
  /// it's being written to the backing file. This ensures that every cache entry starts
  /// at a 4KB offset in the backing file, making hole punching easier as the entire page
  /// can be reclaimed.
  ///
  /// An entry may not be installed for various reasons:
  /// - an entry with the given cache key already exists unless 'buffer_len' is larger
  ///   than the existing entry, in which case, the entry will be replaced with the
  ///   new data.
  /// - a pending entry with the same key is already being installed.
  /// - the maximum write concurrency (via --data_cache_write_concurrency) is reached.
  /// - IO error when writing to the backing file.
  ///
  /// Returns true iff the entry is installed successfully.
  ///
  bool Store(const std::string& filename, int64_t mtime, int64_t offset,
      const uint8_t* buffer, int64_t buffer_len);

  /// Utility function to verify that all partitions' consumption don't exceed their
  /// quotas. Return error status if checking files' sizes failed or if the total space
  /// consumed by a partition exceeded its capacity. Will close the backing files of
  /// partitions before verifying their sizes. Used by test only.
  Status CloseFilesAndVerifySizes();

  /// Set the data cache to read-only. After this function is called, all Store() calls
  /// return false immediately, and for ongoing Store(), this function blocks until they
  /// are complete.
  /// Return current number of writes after read-only set for testing.
  int64_t SetDataCacheReadOnly();

  /// Revoke the data cache read-only.
  /// Return current number of writes after read-only revoked for testing.
  int64_t RevokeDataCacheReadOnly();

  /// Dump the metadata of each cache partition to the same directory on disk as the cache
  /// files, and the dump file can be reloaded back when data cache init. Note that the
  /// data cache will set to read-only mode before the data is dumped.
  Status Dump();

 private:
  friend class DataCacheBaseTest;
  friend class DataCacheTest;
  FRIEND_TEST(DataCacheTest, RotationalDisk);
  FRIEND_TEST(DataCacheTest, NonRotationalDisk);
  FRIEND_TEST(DataCacheTest, InvalidDisk);

  class CacheFile;
  struct CacheKey;
  class CacheEntry;
  class StoreTask;

  /// An implementation of a cache partition. Each partition maintains its own set of
  /// cache keys in a LRU cache.
  class Partition : public Cache::EvictionCallback {
   public:
    /// Creates a partition at the given directory 'path' with quota 'capacity' in bytes.
    /// 'max_opened_files' is the maximum number of opened files allowed per partition.
    /// If 'trace_replay' is true, this only performs metadata operations for the
    /// access trace functionality.
    Partition(int32_t index, const std::string& path, int64_t capacity,
        int max_opened_files, bool trace_replay);

    ~Partition();

    /// Initializes the current partition:
    /// - verifies if the specified directory is valid
    /// - removes any stale backing file in this partition
    /// - checks if there is enough storage space
    /// - checks if the filesystem supports hole punching
    /// - try to load dump file or creates an empty backing file.
    ///
    /// Returns error if there is any of the above steps fails. Returns OK otherwise.
    Status Init();

    /// Close and delete all backing files created for this partition. Also releases
    /// the memory held by the metadata cache.
    void ReleaseResources();

    /// Looks up in the meta-data cache with key 'cache_key'. If found, try copying
    /// 'bytes_to_read' bytes from the backing file into 'buffer'. If trace_replay
    /// is enabled, the buffer is null and no bytes are copied. Returns number
    /// of bytes read from the cache. Returns 0 if there is a cache miss.
    int64_t Lookup(const CacheKey& cache_key, int64_t bytes_to_read, uint8_t* buffer);

    /// Inserts a entry with key 'cache_key' and data in 'buffer' into the cache.
    /// 'buffer' is nullptr for trace replay. 'buffer_len' is the length of buffer.
    /// 'start_reclaim' is set to true if the number of backing files exceeds the per
    /// partition limit. Returns true if the entry is inserted. Returns false otherwise.
    bool Store(const CacheKey& cache_key, const uint8_t* buffer, int64_t buffer_len,
        bool* start_reclaim);

    /// Callback invoked when evicting an entry from the cache. 'key' is the cache key
    /// of the entry being evicted and 'value' contains the cache entry which is the
    /// meta-data of where the cached data is stored.
    virtual void EvictedEntry(kudu::Slice key, kudu::Slice value) override;

    /// Utility function to verify that the backing files don't exceed the capacity
    /// of the partition. Used by test only.
    ///
    /// Return error status if:
    /// - getting files' sizes failed
    /// - total space consumed exceeded the partition's capacity
    /// - file's size exceeded the specified limit in --data_cache_file_max_size
    ///
    /// Will close the backing files of partitions before verifying their sizes.
    ///
    /// Returns OK otherwise.
    Status CloseFilesAndVerifySizes();

    /// Deletes old backing files until number of backing files is no larger than
    /// --data_cache_max_opened_files.
    void DeleteOldFiles();

    /// Set all cache files of this partition to read-only.
    void SetCacheFilesReadOnly();

    /// Revoke all cache files of this partition read-only.
    void RevokeCacheFilesReadOnly();

    /// Dump the 'meta_cache_' and 'cache_files_' of this partition as dump file.
    Status Dump();

   private:
    friend class DataCacheBaseTest;
    friend class DataCacheTest;
    FRIEND_TEST(DataCacheTest, RotationalDisk);
    FRIEND_TEST(DataCacheTest, NonRotationalDisk);
    FRIEND_TEST(DataCacheTest, InvalidDisk);

    class DumpData;

    /// Index of this partition. This is used for naming metrics or other items that
    /// need separate values for each partition. It does not impact cache behavior.
    int32_t index_;

    /// The directory path which this partition stores cached data in.
    const std::string path_;

    /// The capacity in bytes of this partition.
    const int64_t capacity_;

    /// Maximum number of opened files allowed in a partition.
    const int max_opened_files_;

    /// Device-specific write concurrency
    int32_t data_cache_write_concurrency_ = 1;

    /// Whether this is only a trace replay. For trace replay, this is only
    /// performing the metadata operations to determine the hit/miss rate.
    /// There is no need to perform any filesystem operations.
    bool trace_replay_;

    /// True if this partition has been closed. Expected to be set after all IO
    /// threads have been joined.
    bool closed_ = false;

    /// The prefix of the names of the cache backing files.
    static const char* CACHE_FILE_PREFIX;

    /// The file name of the data cache dump file.
    static const char* DUMP_FILE_NAME;

    /// Protects the following fields.
    SpinLock lock_;

    /// If it is true, no file deletion should take place. Set before the files are set to
    /// read only.
    bool files_readonly_ = false;

    /// Index into 'cache_files_' of the oldest opened file.
    int oldest_opened_file_ = -1;

    /// The set of backing files used by this partition. By default, cache_files_.back()
    /// is the latest backing file to which new data is appended. Must be accessed with
    /// 'lock_' held.
    std::vector<std::unique_ptr<CacheFile>> cache_files_;

    /// This set tracks cache keys of entries in progress of being inserted into the
    /// cache. As we don't hold locks while writing to the backing file, this set is
    /// used to prevent multiple insertion into the cache with the same cache key.
    /// The insertion path will check against this set and if the entry doesn't already
    /// exist, it will insert one into this set. Upon completion of cache insertion,
    /// the entry will be removed from this set. Must be accessed with 'lock_' held.
    std::unordered_set<std::string> pending_insert_set_;

    /// The LRU cache for tracking the cache key to cache entries mappings.
    ///
    /// A cache key is created by calling the constructor of CacheKey, which is a tuple
    /// of (fname, mtime, offset).
    ///
    /// A cache entry has type CacheEntry and it contains the metadata of the cached
    /// content. Please see comments at CachedEntry for details.
    std::unique_ptr<Cache> meta_cache_;

    std::unique_ptr<trace::Tracer> tracer_;

    /// Metrics to track performance of the underlying filesystem for the data cache
    /// These are all latency histograms for the operations on the data cache files for
    /// this partition.
    HistogramMetric* read_latency_ = nullptr;
    HistogramMetric* write_latency_ = nullptr;
    HistogramMetric* eviction_latency_ = nullptr;

    /// Initialize the metrics
    void InitMetrics();

    /// Utility function for creating a new backing file in 'path_'. The cache
    /// partition's lock needs to be held when calling this function. Returns
    /// error on failure.
    Status CreateCacheFile();

    /// Utility function to delete cache files that are not tracked by cache_files_ and
    /// may have been left over from previous runs of Impala.
    /// Returns error on failure.
    Status DeleteUntrackedFiles() const;

    /// Utility function for computing the checksum of 'buffer' with length 'buffer_len'.
    static uint64_t Checksum(const uint8_t* buffer, int64_t buffer_len);

    /// Helper function which handles the case in which the key to be inserted already
    /// exists in the cache. With checksumming enabled, it also verifies that the content
    /// in 'buffer' matches the expected checksum in the cache's metadata. Please note
    /// that an existing entry may be overwritten if 'buffer_len' is larger than the
    /// length of the existing entry. 'handle' is the handle into the metadata cache.
    /// Needed for referencing the cache entry.
    ///
    /// Returns true iff the existing entry already covers the range of 'buffer' so no
    /// work needs to be done. Returns false otherwise. In which case, the existing entry
    /// will be overwritten.
    bool HandleExistingEntry(const kudu::Slice& key,
        const Cache::UniqueHandle& handle, const uint8_t* buffer,
        int64_t buffer_len);

    /// Helper function to insert a new entry with key 'key' into the LRU cache.
    /// The content in 'buffer' of length 'buffer_len' in bytes will be written to
    /// the backing file 'cache_file' at offset 'insertion_offset'.
    ///
    /// Returns true iff the insertion into the cache and the write to the backing file
    /// succeeded. Returns false otherwise.
    bool InsertIntoCache(const kudu::Slice& key, CacheFile* cache_file,
        int64_t insertion_offset, const uint8_t* buffer, int64_t buffer_len);

    /// Utility function for verifying that the checksum of 'buffer' with length
    /// 'buffer_len' matches the checksum recorded in the meta-data 'entry->checksum'.
    ///
    /// 'ops_name" is the name of the operation which triggers the checksum verification.
    /// Currently, it's either "read" or "write" but future changes may add more names.
    ///
    /// Returns false if the checksum of 'buffer' doesn't match 'entry->checksum'.
    static bool VerifyChecksum(const std::string& ops_name, const CacheEntry& entry,
        const uint8_t* buffer, int64_t buffer_len);

    /// Load the 'cache_files_' and 'meta_cache_' of this partition from dump file.
    Status Load();

    /// Dump the 'cache_files_' of this partition into 'dump_data'.
    Status DumpCacheFiles(DumpData& dump_data);

    /// Load the 'cache_files_' for this partition from 'dump_data'.
    Status LoadCacheFiles(const DumpData& dump_data);

    /// Dump the 'meta_cache_' of this partition into 'dump_data'.
    Status DumpMetaCache(DumpData& dump_data);

    /// Load the 'meta_cache_' for this partition from 'dump_data'.
    Status LoadMetaCache(const DumpData& dump_data);

    void Trace(const trace::EventType& status, const DataCache::CacheKey& key,
        int64_t lookup_len, int64_t entry_len);
  };

  /// The configuration string for the data cache.
  const std::string config_;

  /// The capacity in bytes of one partition.
  int64_t per_partition_capacity_;

  /// Set to true if this is only doing trace replay. Trace replay does only metadata
  /// operations, and no filesystem operations are required.
  bool trace_replay_;

  /// This lock keep SetDataCacheReadOnly() blocked until all ongoing Store() complete.
  boost::shared_mutex readonly_lock_;

  /// Store() will return false immediately if this is true.
  AtomicBool readonly_{false};

  /// The set of all cache partitions.
  std::vector<std::unique_ptr<Partition>> partitions_;

  /// Thread pool for deleting old files from partitions to keep the number of opened
  /// files within --date_cache_max_opened_files. This allows deletion requests
  /// to be queued for deferred processing. There is only one thread in this pool.
  std::unique_ptr<ThreadPool<int>> file_deleter_pool_;

  /// Thread function called by threads in 'file_deleter_pool_' for deleting old files
  /// in partitions_[partition_idx].
  void DeleteOldFiles(uint32_t thread_id, int partition_idx);

  /// Create a new store task and copy the data to a temporary buffer, then submit it to
  /// the asynchronous write thread pool for handling. May abort due to buffer size limit.
  /// Return true if success.
  bool SubmitStoreTask(const std::string& filename, int64_t mtime, int64_t offset,
      const uint8_t* buffer, int64_t buffer_len);

  /// Called by StoreTask's d'tor, decrease the current_buffer_size_ by task's buffer_len.
  void CompleteStoreTask(const StoreTask& task);

  /// Thread pool for storing cache asynchronously, it is initialized only if
  /// 'data_cache_num_async_write_threads' has been set above 0, and creates a
  /// corresponding number of worker threads.
  int32_t num_async_write_threads_;
  using StoreTaskHandle = std::unique_ptr<const StoreTask>;
  std::unique_ptr<ThreadPool<StoreTaskHandle>> storer_pool_;

  /// Thread function called by threads in 'storer_pool_' for handling store task.
  void HandleStoreTask(uint32_t thread_id, const StoreTaskHandle& task);

  /// Limit of the total buffer size used by asynchronous store tasks, when the current
  /// buffer size reaches the limit, the subsequent store task will be abandoned.
  int64_t store_buffer_capacity_;

  /// Total buffer size currently used by all asynchronous store tasks.
  AtomicInt64 current_buffer_size_{0};

  /// Call the corresponding cache partition for storing.
  bool StoreInternal(const CacheKey& key, const uint8_t* buffer, int64_t buffer_len);

};

} // namespace io
} // namespace impala


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


#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_BASE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_BASE_H_

#include <stdint.h>
#include <memory>
#include <unordered_set>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

#include "exec/filter-context.h"
#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "util/avro-util.h"
#include "util/progress-updater.h"
#include "util/spinlock.h"

namespace impala {

class ScannerContext;
class DescriptorTbl;
class HdfsScanner;
class RowBatch;
class Status;
class Tuple;
class TPlanNode;
class TScanRange;

/// Maintains per file information for files assigned to this scan node.  This includes
/// all the splits for the file. Note that it is not thread-safe.
struct HdfsFileDesc {
  HdfsFileDesc(const std::string& filename)
    : fs(NULL), filename(filename), file_length(0), mtime(0),
      file_compression(THdfsCompression::NONE) {
  }

  /// Connection to the filesystem containing the file.
  hdfsFS fs;

  /// File name including the path.
  std::string filename;

  /// Length of the file. This is not related to which parts of the file have been
  /// assigned to this node.
  int64_t file_length;

  /// Last modified time
  int64_t mtime;

  THdfsCompression::type file_compression;

  /// Splits (i.e. raw byte ranges) for this file, assigned to this scan node.
  std::vector<DiskIoMgr::ScanRange*> splits;
};

/// Struct for additional metadata for scan ranges. This contains the partition id
/// that this scan range is for.
struct ScanRangeMetadata {
  /// The partition id that this range is part of.
  int64_t partition_id;

  /// For parquet scan ranges we initially create a request for the file footer for each
  /// split; we store a pointer to the actual split so that we can recover its information
  /// for the scanner to process.
  const DiskIoMgr::ScanRange* original_split;

  ScanRangeMetadata(int64_t partition_id, const DiskIoMgr::ScanRange* original_split)
      : partition_id(partition_id), original_split(original_split) { }
};


/// Base class for all Hdfs scan nodes. Contains common members and functions
/// that are independent of whether batches are materialized by the main thread
/// (via HdfsScanner::GexNext()) or by spinning up separate threads that feed
/// into a RowBatch queue (via HdfsScanner::ProcessSplit()). Those specifics
/// are expected to be implemented in subclasses.
///
/// Subclasses may expect to receive runtime filters produced elsewhere in the plan
/// (even from remote fragments). These filters arrive asynchronously during execution,
/// and are applied as soon as they arrive. Filters may be applied by the scan node in
/// the following scopes:
///
/// 1. Per-file (all file formats, partition column filters only) - filtering at this
/// scope saves IO as the filters are applied before scan ranges are issued.
/// 2. Per-scan-range (all file formats, partition column filters only) - filtering at
/// this scope saves CPU as filtered scan ranges are never scanned.
///
/// Scanners may also use the same filters to eliminate rows at finer granularities
/// (e.g. per row).
///
/// TODO: Revisit and minimize metrics. Move those specific to legacy multi-threaded
/// scans into HdfsScanNode.
/// TODO: Once the legacy scan node has been removed, several functions can be made
/// non-virtual. Also merge this class with HdfsScanNodeMt.
class HdfsScanNodeBase : public ScanNode {
 public:
  HdfsScanNodeBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~HdfsScanNodeBase();

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  /// Returns true if this node uses separate threads for scanners that append RowBatches
  /// to a queue, false otherwise.
  virtual bool HasRowBatchQueue() const = 0;

  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  /// Returns the tuple idx into the row for this scan node to output to.
  /// Currently this is always 0.
  int tuple_idx() const { return 0; }

  /// Returns number of partition keys in the table.
  int num_partition_keys() const { return hdfs_table_->num_clustering_cols(); }

  /// Returns number of partition key slots.
  int num_materialized_partition_keys() const { return partition_key_slots_.size(); }

  int min_max_tuple_id() const { return min_max_tuple_id_; }

  const std::vector<ExprContext*> min_max_conjunct_ctxs() const {
    return min_max_conjunct_ctxs_;
  }

  const TupleDescriptor* min_max_tuple_desc() const { return min_max_tuple_desc_; }
  const TupleDescriptor* tuple_desc() const { return tuple_desc_; }
  const HdfsTableDescriptor* hdfs_table() { return hdfs_table_; }
  const AvroSchemaElement& avro_schema() { return *avro_schema_.get(); }
  RuntimeState* runtime_state() { return runtime_state_; }
  int skip_header_line_count() const { return skip_header_line_count_; }
  DiskIoRequestContext* reader_context() { return reader_context_; }

  typedef std::map<TupleId, std::vector<ExprContext*>> ConjunctsMap;
  const ConjunctsMap& conjuncts_map() const { return conjuncts_map_; }

  /// Slot Id => Dictionary Filter eligible conjuncts for that slot
  typedef std::map<SlotId, std::vector<ExprContext*>> DictFilterConjunctsMap;
  const DictFilterConjunctsMap& dict_filter_conjuncts_map() const {
    return dict_filter_conjuncts_map_;
  }

  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length() {
    return max_compressed_text_file_length_;
  }

  const static int SKIP_COLUMN = -1;

  /// Returns index into materialized_slots with 'path'.  Returns SKIP_COLUMN if
  /// that path is not materialized.
  int GetMaterializedSlotIdx(const std::vector<int>& path) const {
    PathToSlotIdxMap::const_iterator result = path_to_materialized_slot_idx_.find(path);
    if (result == path_to_materialized_slot_idx_.end()) return SKIP_COLUMN;
    return result->second;
  }

  /// The result array is of length hdfs_table_->num_cols(). The i-th element is true iff
  /// column i should be materialized.
  const bool* is_materialized_col() {
    return reinterpret_cast<const bool*>(&is_materialized_col_[0]);
  }

  /// Returns the per format codegen'd function.  Scanners call this to get the
  /// codegen'd function to use.  Returns NULL if codegen should not be used.
  void* GetCodegenFn(THdfsFileFormat::type);

  inline void IncNumScannersCodegenEnabled() { num_scanners_codegen_enabled_.Add(1); }
  inline void IncNumScannersCodegenDisabled() { num_scanners_codegen_disabled_.Add(1); }

  /// Allocate a new scan range object, stored in the runtime state's object pool. For
  /// scan ranges that correspond to the original hdfs splits, the partition id must be
  /// set to the range's partition id. For other ranges (e.g. columns in parquet, read
  /// past buffers), the partition_id is unused. expected_local should be true if this
  /// scan range is not expected to require a remote read. The range must fall within
  /// the file bounds. That is, the offset must be >= 0, and offset + len <= file_length.
  /// If not NULL, the 'original_split' pointer is stored for reference in the scan range
  /// metadata of the scan range that is to be allocated.
  /// This is thread safe.
  DiskIoMgr::ScanRange* AllocateScanRange(hdfsFS fs, const char* file, int64_t len,
      int64_t offset, int64_t partition_id, int disk_id, bool expected_local,
      const DiskIoMgr::BufferOpts& buffer_opts,
      const DiskIoMgr::ScanRange* original_split = NULL);

  /// Old API for compatibility with text scanners (e.g. LZO text scanner).
  DiskIoMgr::ScanRange* AllocateScanRange(hdfsFS fs, const char* file, int64_t len,
      int64_t offset, int64_t partition_id, int disk_id, bool try_cache,
      bool expected_local, int mtime, const DiskIoMgr::ScanRange* original_split = NULL);

  /// Adds ranges to the io mgr queue. 'num_files_queued' indicates how many file's scan
  /// ranges have been added completely.  A file's scan ranges are added completely if no
  /// new scanner threads will be needed to process that file besides the additional
  /// threads needed to process those in 'ranges'.
  /// Can be overridden to add scan-node specific actions like starting scanner threads.
  virtual Status AddDiskIoRanges(const std::vector<DiskIoMgr::ScanRange*>& ranges,
      int num_files_queued);

  /// Adds all splits for file_desc to the io mgr queue and indicates one file has
  /// been added completely.
  inline Status AddDiskIoRanges(const HdfsFileDesc* file_desc) {
    return AddDiskIoRanges(file_desc->splits, 1);
  }

  /// Allocates and initializes a new template tuple allocated from pool with values
  /// from the partition columns for the current scan range, if any,
  /// Returns NULL if there are no partition keys slots.
  Tuple* InitTemplateTuple(const std::vector<ExprContext*>& value_ctxs,
      MemPool* pool, RuntimeState* state) const;

  /// Returns the file desc for 'filename'.  Returns NULL if filename is invalid.
  HdfsFileDesc* GetFileDesc(const std::string& filename);

  /// Sets the scanner specific metadata for 'filename'. Scanners can use this to store
  /// file header information. Thread safe.
  void SetFileMetadata(const std::string& filename, void* metadata);

  /// Returns the scanner specific metadata for 'filename'. Returns NULL if there is no
  /// metadata. Thread safe.
  void* GetFileMetadata(const std::string& filename);

  /// Called by scanners when a range is complete. Used to record progress.
  /// This *must* only be called after a scanner has completely finished its
  /// scan range (i.e. context->Flush()), and has returned the final row batch.
  /// Otherwise, scan nodes using a RowBatch queue may lose the last batch due
  /// to racing with shutting down the queue.
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const THdfsCompression::type& compression_type);
  /// Same as above except for when multiple compression codecs were used
  /// in the file. The metrics are incremented for each compression_type.
  virtual void RangeComplete(const THdfsFileFormat::type& file_type,
      const std::vector<THdfsCompression::type>& compression_type);

  /// Utility function to compute the order in which to materialize slots to allow for
  /// computing conjuncts as slots get materialized (on partial tuples).
  /// 'order' will contain for each slot, the first conjunct it is associated with.
  /// e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  /// evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  /// order set to conjuncts.size()
  void ComputeSlotMaterializationOrder(std::vector<int>* order) const;

  /// Returns true if there are no materialized slots, such as a count(*) over the table.
  inline bool IsZeroSlotTableScan() const {
    return materialized_slots().empty() && tuple_desc()->tuple_path().empty();
  }

  /// map from volume id to <number of split, per volume split lengths>
  typedef boost::unordered_map<int32_t, std::pair<int, int64_t>> PerVolumnStats;

  /// Update the per volume stats with the given scan range params list
  static void UpdateHdfsSplitStats(
      const std::vector<TScanRangeParams>& scan_range_params_list,
      PerVolumnStats* per_volume_stats);

  /// Output the per_volume_stats to stringstream. The output format is a list of:
  /// <volume id>:<# splits>/<per volume split lengths>
  static void PrintHdfsSplitStats(const PerVolumnStats& per_volume_stats,
      std::stringstream* ss);

  /// Description string for the per volume stats output.
  static const std::string HDFS_SPLIT_STATS_DESC;

  /// Returns true if partition 'partition_id' passes all the filter predicates in
  /// 'filter_ctxs' and should not be filtered out. 'stats_name' is the key of one of the
  /// counter groups in FilterStats, and is used to update the correct statistics.
  ///
  /// 'filter_ctxs' is either an empty list, in which case filtering is disabled and the
  /// function returns true, or a set of filter contexts to evaluate.
  bool PartitionPassesFilters(int32_t partition_id, const std::string& stats_name,
      const std::vector<FilterContext>& filter_ctxs);

  const std::vector<FilterContext>& filter_ctxs() const { return filter_ctxs_; }

 protected:
  friend class ScannerContext;
  friend class HdfsScanner;

  RuntimeState* runtime_state_;

  /// Tuple id of the tuple used to evaluate conjuncts on parquet::Statistics.
  const int min_max_tuple_id_;

  /// Conjuncts to evaluate on parquet::Statistics.
  vector<ExprContext*> min_max_conjunct_ctxs_;

  /// Descriptor for the tuple used to evaluate conjuncts on parquet::Statistics.
  TupleDescriptor* min_max_tuple_desc_;

  // Number of header lines to skip at the beginning of each file of this table. Only set
  // to values > 0 for hdfs text files.
  const int skip_header_line_count_;

  /// Tuple id resolved in Prepare() to set tuple_desc_
  const int tuple_id_;

  /// RequestContext object to use with the disk-io-mgr for reads.
  DiskIoRequestContext* reader_context_;

  /// Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_;

  /// Map from partition ID to a template tuple (owned by scan_node_pool_) which has only
  /// the partition columns for that partition materialized. Used to filter files and scan
  /// ranges on partition-column filters. Populated in Prepare().
  boost::unordered_map<int64_t, Tuple*> partition_template_tuple_map_;

  /// Descriptor for the hdfs table, including partition and format metadata.
  /// Set in Prepare, owned by RuntimeState
  const HdfsTableDescriptor* hdfs_table_;

  /// The root of the table's Avro schema, if we're scanning an Avro table.
  ScopedAvroSchemaElement avro_schema_;

  /// Partitions scanned by this scan node.
  std::unordered_set<int64_t> partition_ids_;

  /// File path => file descriptor (which includes the file's splits)
  typedef std::map<std::string, HdfsFileDesc*> FileDescMap;
  FileDescMap file_descs_;

  /// File format => file descriptors.
  typedef std::map<THdfsFileFormat::type, std::vector<HdfsFileDesc*>> FileFormatsMap;
  FileFormatsMap per_type_files_;

  /// Scanner specific per file metadata (e.g. header information) and associated lock.
  /// TODO: Remove this lock when removing the legacy scanners and scan nodes.
  boost::mutex metadata_lock_;
  std::map<std::string, void*> per_file_metadata_;

  /// Conjuncts for each materialized tuple (top-level row batch tuples and collection
  /// item tuples). Includes a copy of ExecNode.conjuncts_.
  ConjunctsMap conjuncts_map_;

  /// Dictionary filtering eligible conjuncts for each slot
  DictFilterConjunctsMap dict_filter_conjuncts_map_;

  /// Set to true when the initial scan ranges are issued to the IoMgr. This happens on
  /// the first call to GetNext(). The token manager, in a different thread, will read
  /// this variable.
  bool initial_ranges_issued_;

  /// Number of files that have not been issued from the scanners.
  AtomicInt32 num_unqueued_files_;

  /// Per scanner type codegen'd fn.
  typedef std::map<THdfsFileFormat::type, void*> CodegendFnMap;
  CodegendFnMap codegend_fn_map_;

  /// Maps from a slot's path to its index into materialized_slots_.
  typedef boost::unordered_map<std::vector<int>, int> PathToSlotIdxMap;
  PathToSlotIdxMap path_to_materialized_slot_idx_;

  /// List of contexts for expected runtime filters for this scan node. These contexts are
  /// cloned by individual scanners to be used in multi-threaded contexts, passed through
  /// the per-scanner ScannerContext..
  std::vector<FilterContext> filter_ctxs_;

  /// is_materialized_col_[i] = <true i-th column should be materialized, false otherwise>
  /// for 0 <= i < total # columns in table
  //
  /// This should be a vector<bool>, but bool vectors are special-cased and not stored
  /// internally as arrays, so instead we store as chars and cast to bools as needed
  std::vector<char> is_materialized_col_;

  /// Vector containing slot descriptors for all non-partition key slots.  These
  /// descriptors are sorted in order of increasing col_pos.
  std::vector<SlotDescriptor*> materialized_slots_;

  /// Vector containing slot descriptors for all partition key slots.
  std::vector<SlotDescriptor*> partition_key_slots_;

  /// Keeps track of total splits and the number finished.
  ProgressUpdater progress_;

  /// Counters which track the number of scanners that have codegen enabled for the
  /// materialize and conjuncts evaluation code paths.
  AtomicInt32 num_scanners_codegen_enabled_;
  AtomicInt32 num_scanners_codegen_disabled_;

  /// This is the number of io buffers that are owned by the scan node and the scanners.
  /// This is used just to help debug leaked io buffers to determine if the leak is
  /// happening in the scanners vs other parts of the execution.
  /// TODO: Remove this counter when deprecating the multi-threaded scan node.
  AtomicInt32 num_owned_io_buffers_;

  /// If true, counters are actively running and need to be reported in the runtime
  /// profile.
  bool counters_running_;

  /// The size of the largest compressed text file to be scanned. This is used to
  /// estimate scanner thread memory usage.
  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length_;

  /// Disk accessed bitmap
  RuntimeProfile::Counter disks_accessed_bitmap_;

  /// Total number of bytes read locally
  RuntimeProfile::Counter* bytes_read_local_;

  /// Total number of bytes read via short circuit read
  RuntimeProfile::Counter* bytes_read_short_circuit_;

  /// Total number of bytes read from data node cache
  RuntimeProfile::Counter* bytes_read_dn_cache_;

  /// Total number of remote scan ranges
  RuntimeProfile::Counter* num_remote_ranges_;

  /// Total number of bytes read remotely that were expected to be local
  RuntimeProfile::Counter* unexpected_remote_bytes_;

  /// Pool for allocating some amounts of memory that is shared between scanners.
  /// e.g. partition key tuple and their string buffers
  boost::scoped_ptr<MemPool> scan_node_pool_;

  /// Status of failed operations.  This is set in the ScannerThreads
  /// Returned in GetNext() if an error occurred.  An non-ok status triggers cleanup
  /// scanner threads.
  Status status_;

  /// Mapping of file formats (file type, compression type) to the number of
  /// splits of that type and the lock protecting it.
  typedef std::map<
      std::pair<THdfsFileFormat::type, THdfsCompression::type>, int> FileTypeCountsMap;
  FileTypeCountsMap file_type_counts_;

  /// Performs dynamic partition pruning, i.e., applies runtime filters to files, and
  /// issues initial ranges for all file types. Waits for runtime filters if necessary.
  /// Only valid to call if !initial_ranges_issued_. Sets initial_ranges_issued_ to true.
  Status IssueInitialScanRanges(RuntimeState* state);

  /// Create and open new scanner for this partition type.
  /// If the scanner is successfully created and opened, it is returned in 'scanner'.
  Status CreateAndOpenScanner(HdfsPartitionDescriptor* partition,
      ScannerContext* context, boost::scoped_ptr<HdfsScanner>* scanner);

  /// Recursively initializes all NULL collection slots to an empty CollectionValue in
  /// addition to maintaining the null bit. Hack to allow UnnestNode to project out
  /// collection slots. Assumes that the null bit has already been un/set.
  /// TODO: Remove this function once the TODOs in UnnestNode regarding projection
  /// have been addressed.
  void InitNullCollectionValues(const TupleDescriptor* tuple_desc, Tuple* tuple) const;

  /// Helper to call InitNullCollectionValues() on all tuples produced by this scan
  /// in 'row_batch'.
  void InitNullCollectionValues(RowBatch* row_batch) const;

  /// Returns false if, according to filters in 'filter_ctxs', 'file' should be filtered
  /// and therefore not processed. 'file_type' is the the format of 'file', and is used
  /// for bookkeeping. Returns true if all filters pass or are not present.
  bool FilePassesFilterPredicates(const std::vector<FilterContext>& filter_ctxs,
      const THdfsFileFormat::type& file_type, HdfsFileDesc* file);

  /// Waits for up to time_ms for runtime filters to arrive, checking every 20ms. Returns
  /// true if all filters arrived within the time limit (as measured from the time of
  /// RuntimeFilterBank::RegisterFilter()), false otherwise.
  bool WaitForRuntimeFilters(int32_t time_ms);

  /// Stops periodic counters and aggregates counter values for the entire scan node.
  /// This should be called as soon as the scan node is complete to get the most accurate
  /// counter values.
  /// This can be called multiple times, subsequent calls will be ignored.
  /// This must be called on Close() to unregister counters.
  /// Scan nodes with a RowBatch queue may have to synchronize calls to this function.
  void StopAndFinalizeCounters();

  /// Calls ExecDebugAction(). Returns the status based on the debug action specified
  /// for the query.
  Status TriggerDebugAction();
};

}

#endif

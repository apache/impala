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
#include <tuple>
#include <unordered_set>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>

#include "codegen/codegen-fn-ptr.h"
#include "exec/file-metadata-utils.h"
#include "exec/filter-context.h"
#include "exec/scan-node.h"
#include "exec/scan-range-queue-mt.h"
#include "runtime/descriptors.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "util/avro-util.h"
#include "util/container-util.h"
#include "util/progress-updater.h"
#include "util/spinlock.h"
#include "util/unique-id-hash.h"

namespace org { namespace apache { namespace impala { namespace fb {
struct FbFileMetadata;
}}}}

namespace impala {

class ScannerContext;
class DescriptorTbl;
class HdfsScanner;
class HdfsScanPlanNode;
class RowBatch;
class Status;
class Tuple;
class TPlanNode;
class TScanRange;

/// Maintains per file information for files assigned to this scan node. This includes
/// all the splits for the file. Note that it is not thread-safe.
struct HdfsFileDesc {
  HdfsFileDesc(const std::string& filename)
    : fs(NULL),
      filename(filename),
      file_length(0),
      mtime(0),
      file_compression(THdfsCompression::NONE),
      file_format(THdfsFileFormat::TEXT) {}

  io::ScanRange::FileInfo GetFileInfo() const {
    return io::ScanRange::FileInfo{
        filename.c_str(), fs, mtime, is_encrypted, is_erasure_coded};
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
  THdfsFileFormat::type file_format;

  /// Splits (i.e. raw byte ranges) for this file, assigned to this scan node.
  std::vector<io::ScanRange*> splits;

  /// Extra file metadata, e.g. Iceberg-related file-level info.
  const ::org::apache::impala::fb::FbFileMetadata* file_metadata;

  /// Whether file is encrypted.
  bool is_encrypted = false;

  /// Whether file is erasure coded.
  bool is_erasure_coded = false;

  /// Some useful typedefs for creating HdfsFileDesc related data structures.
  /// This is a pair for partition ID and filename which uniquely identifies a file.
  typedef pair<int64_t, std::string> PartitionFileKey;
  /// Partition_id, File path => file descriptor
  typedef std::unordered_map<PartitionFileKey, HdfsFileDesc*, pair_hash> FileDescMap;
  /// File format => file descriptors.
  typedef std::map<THdfsFileFormat::type, std::vector<HdfsFileDesc*>>
    FileFormatsMap;
};

/// Struct for additional metadata for scan ranges. This contains the partition id
/// that this scan range is for.
struct ScanRangeMetadata {
  /// The partition id that this range is part of.
  int64_t partition_id;

  /// For parquet scan ranges we initially create a request for the file footer for each
  /// split; we store a pointer to the actual split so that we can recover its information
  /// for the scanner to process.
  const io::ScanRange* original_split;

  /// True, if this object belongs to a scan range which is the header of a
  /// sequence-based file
  bool is_sequence_header = false;

  ScanRangeMetadata(int64_t partition_id, const io::ScanRange* original_split)
      : partition_id(partition_id), original_split(original_split) { }
};

/// Encapsulated all mutable state related to scan ranges that is shared across all scan
/// node instances. Maintains the following context that can be accessed by all instances
/// in a thread safe manner:
/// - File Descriptors
/// - File Metadata
/// - Template Tuples for partitions
/// - ProgressUpdater keeping tracks of how many scan ranges have been read.
/// - Counter tracking remaining scan range submissions
/// - API for fetching and adding a scan range to the queue.
///
/// An instance of this can be created and initialized exclusively by HdfsScanPlanNode.
class ScanRangeSharedState {
 public:
  /// Given a partition_id and filename returns the related file descriptor DCHECK ensures
  /// there is always file descriptor returned.
  const HdfsFileDesc* GetFileDesc(int64_t partition_id, const std::string& filename);

  /// Sets the scanner specific metadata for 'partition_id' and 'filename'.
  /// Scanners can use this to store file header information. Thread safe.
  void SetFileMetadata(
      int64_t partition_id, const std::string& filename, void* metadata);

  /// Returns the scanner specific metadata for 'partition_id' and 'filename'.
  /// Returns nullptr if there is no metadata. Thread safe.
  void* GetFileMetadata(int64_t partition_id, const std::string& filename);

  /// Get the template tuple which has only the partition columns materialized for the
  /// partition identified by 'partition_id' .
  Tuple* GetTemplateTupleForPartitionId(int64_t partition_id);

  ObjectPool* obj_pool() { return &obj_pool_; }
  ProgressUpdater& progress() { return progress_; }
  HdfsFileDesc::FileFormatsMap& per_type_files() { return per_type_files_; }

  /// Updates the counter keeping track of remaining scan range submissions. If MT scan
  /// nodes are being used, it notifies all instances waiting in GetNextScanRange() every
  /// time it updates.
  void UpdateRemainingScanRangeSubmissions(int32_t delta);

  /// Returns the number of remaining scan range submissions.
  int RemainingScanRangeSubmissions() {
    return remaining_scan_range_submissions_.Load();
  }

  /// Returns the list of files assigned to the fragment with id 'fragment_instance_id'
  /// for which initial scan ranges need to be issued. Returns nullptr if the instance
  /// has not been assigned anything.
  std::vector<HdfsFileDesc*>* GetFilesForIssuingScanRangesForInstance(
      const TUniqueId fragment_instance_id) {
    auto it = file_assignment_per_instance_.find(fragment_instance_id);
    if (it == file_assignment_per_instance_.end()) return nullptr;
    return &it->second;
  }

  /// The following public methods are only used by MT scan nodes.

  /// Adds all scan ranges to the queue. If 'at_front' is true or the range has
  /// USE_HDFS_CACHE option set, then adds it to the front of the queue.
  void EnqueueScanRange(const std::vector<io::ScanRange*>& ranges, bool at_front);

  /// Sets a reference to the next scan range in input variable 'scan_range' from a queue
  /// of scan ranges that need to be read. Blocks if there are remaining scan range
  /// submissions and the queue is empty. Unblocks and returns CANCELLED status in case
  /// the query was cancelled. 'scan_range' is set to nullptr if no more scan ranges are
  /// left to read.
  Status GetNextScanRange(RuntimeState* state, io::ScanRange** scan_range);

  /// Add the required hooks to the runtime state that gets triggered in case of
  /// cancellation. Must be called before adding or removing scan ranges to the queue.
  void AddCancellationHook(RuntimeState* state);

  /// Transfers all memory from 'pool' to 'template_pool_'.
  void TransferToSharedStatePool(MemPool* pool);

 private:
  friend class HdfsScanPlanNode;

  ScanRangeSharedState() = default;
  DISALLOW_COPY_AND_ASSIGN(ScanRangeSharedState);

  /// Contains all the file descriptors and the scan ranges created.
  ObjectPool obj_pool_;

  /// For storing partition template tuples.
  std::unique_ptr<MemPool> template_pool_;

  /// partition_id, File path => file descriptor (which includes the file's splits).
  /// Populated in HdfsScanPlanNode::Init() after which it is never modified.
  HdfsFileDesc::FileDescMap file_descs_;

  /// Scanner specific per file metadata (e.g. header information) and associated lock.
  /// Currently only used by sequence scanners.
  /// Key of the map is partition_id, filename pair
  std::mutex metadata_lock_;
  std::unordered_map<HdfsFileDesc::PartitionFileKey, void*, pair_hash> per_file_metadata_;

  /// Map from partition ID to a template tuple (owned by template_pool_) which has only
  /// the partition columns for that partition materialized. Used to filter files and scan
  /// ranges on partition-column filters. Populated in HdfsScanPlanNode::Init().
  boost::unordered_map<int64_t, Tuple*> partition_template_tuple_map_;

  /// File format => file descriptors.
  HdfsFileDesc::FileFormatsMap per_type_files_;

  /// Contains a list of files for every instance equally divided among all. Indexed by
  /// fragment instance id. Might not contain an entry for all instances in case there are
  /// not enough files to be read.
  std::unordered_map<TUniqueId, std::vector<HdfsFileDesc*>> file_assignment_per_instance_;

  /// Keeps track of total splits remaining to be read.
  ProgressUpdater progress_;

  /// When this counter drops to 0, AddDiskIoRanges() should not be called again, and
  /// therefore scanner threads (for non-MT) or fragment instances (for MT) can't get work
  /// should exit. For most file formats (except for sequence-based formats), this is 0
  /// after IssueInitialRanges() has been called by all fragment instances. Note that some
  /// scanners (namely Parquet) issue additional work to the IO subsystem without using
  /// AddDiskIoRanges(), but that is managed within the scanner, and doesn't require
  /// additional scanner threads.
  /// Initialized the number of instances for this fragment in HdfsScanPlanNode::Init().
  AtomicInt32 remaining_scan_range_submissions_{0};

  /// Indicates whether MT scan nodes are being used.
  bool use_mt_scan_node_ = false;

  /// The following are only used by MT scan nodes.
  /////////////////////////////////////////////////////////////////////
  /// BEGIN: Members that are used only by MT scan nodes(use_mt_scan_node_ is true).

  /// Lock synchronizes access for calling wait on the condition variable.
  std::mutex scan_range_submission_lock_;
  /// Used by scan instances to wait for remaining scan range submission
  ConditionVariable range_submission_cv_;

  /// Queue of all scan ranges that need to be read. Shared by all instances of this
  /// fragment. Only used for MT scans.
  ScanRangeQueueMt scan_range_queue_;

  /// END: Members that are used only by MT scan nodes(use_mt_scan_node_ is true).
  /////////////////////////////////////////////////////////////////////
};

class HdfsScanPlanNode : public ScanPlanNode {
 public:
  virtual Status Init(const TPlanNode& tnode, FragmentState* state) override;
  virtual void Close() override;
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  virtual void Codegen(FragmentState* state) override;

  /// Returns index into materialized_slots with 'path'.  Returns SKIP_COLUMN if
  /// that path is not materialized. Only valid to call after Init().
  int GetMaterializedSlotIdx(const std::vector<int>& path) const;

  /// Utility function to compute the order in which to materialize slots to allow for
  /// computing conjuncts as slots get materialized (on partial tuples).
  /// 'order' will contain for each slot, the first conjunct it is associated with.
  /// e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  /// evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  /// order set to conjuncts.size(). Only valid to call after Init().
  void ComputeSlotMaterializationOrder(
      const DescriptorTbl& desc_tbl, std::vector<int>* order) const;

 /// Returns true if it has a virtual column that we can materialize in the template tuple
  bool HasVirtualColumnInTemplateTuple() const;

  /// Conjuncts for each materialized tuple (top-level row batch tuples and collection
  /// item tuples). Includes a copy of PlanNode.conjuncts_.
  typedef std::unordered_map<TupleId, std::vector<ScalarExpr*>> ConjunctsMap;
  ConjunctsMap conjuncts_map_;

  /// Conjuncts to evaluate on parquet::Statistics or to be pushed down to ORC reader.
  std::vector<ScalarExpr*> stats_conjuncts_;

  /// The list of overlap predicate descs. Each is used to find out whether the range
  /// of values of a data item overlap with a min/max filter. Data structure wise, each
  /// desc is composed of the ID of the min/max filter, the slot index in
  /// 'stats_tuple_desc_' to hold the min value of the data item and the actual overlap
  /// predicate. The next slot after the slot index implicitly holds the max value of
  /// the data item.
  std::vector<TOverlapPredicateDesc> overlap_predicate_descs_;

  /// Tuple id resolved in Init() to set tuple_desc_.
  int tuple_id_ = -1;

  /// Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Maps from a slot's path to its index into materialized_slots_.
  boost::unordered_map<std::vector<int>, int> path_to_materialized_slot_idx_;

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

  /// Vector containing slot descriptors for virtual columns.
  std::vector<SlotDescriptor*> virtual_column_slots_;

  /// Descriptor for the hdfs table, including partition and format metadata.
  /// Set in Init, owned by QueryState
  const HdfsTableDescriptor* hdfs_table_ = nullptr;

  /// The root of the table's Avro schema, if we're scanning an Avro table.
  ScopedAvroSchemaElement avro_schema_;

  /// State related to scan ranges shared across all scan node instances.
  ScanRangeSharedState shared_state_;

  /// Allocates and initializes a new template tuple allocated from pool with values
  /// from the partition columns for the current scan range, if any,
  /// Returns nullptr if there are no partition keys slots.
  Tuple* InitTemplateTuple(
      const std::vector<ScalarExprEvaluator*>& evals, MemPool* pool) const;

  /// Processes all the scan range params for this scan node to create all the required
  /// file descriptors, update metrics and fill in all relevant state in 'shared_state_'.
  Status ProcessScanRangesAndInitSharedState(FragmentState* state);
  /// Per scanner type codegen'd fn.
  /// The actual types of the functions differ so we use void* as the common type and
  /// reinterpret cast before calling the functions.
  typedef boost::unordered_map<THdfsFileFormat::type, CodegenFnPtr<void*>> CodegendFnMap;
  CodegendFnMap codegend_fn_map_;
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
/// Counters:
///
///   TotalRawHdfsReadTime - the total wall clock time spent by Disk I/O threads in HDFS
///     read operations. For example, if we have 3 reading threads and each spent 1 sec,
///     this counter will report 3 sec.
///
///   TotalRawHdfsOpenFileTime - the total wall clock time spent by Disk I/O threads in
///     HDFS open operations.
///
///   PerReadThreadRawHdfsThroughput - the read throughput in bytes/sec for each HDFS
///     read thread while it is executing I/O operations on behalf of this scan.
///
///   NumDisksAccessed - number of distinct disks accessed by HDFS scan. Each local disk
///     is counted as a disk and each remote disk queue (e.g. HDFS remote reads, S3)
///     is counted as a distinct disk.
///
///   ScannerIoWaitTime - total amount of time scanner threads spent waiting for
///     I/O. This is comparable to ScannerThreadsTotalWallClockTime in the traditional
///     HDFS scan nodes and the scan node total time for the MT_DOP > 1 scan nodes.
///     Low values show that each I/O completed before or around the time that the
///     scanner thread was ready to process the data. High values show that scanner
///     threads are spending significant time waiting for I/O instead of processing data.
///     Note that if CPU load is high, this can include time that the thread is runnable
///     but not scheduled.
///
///   AverageHdfsReadThreadConcurrency - the average number of HDFS read threads
///     executing read operations on behalf of this scan. Higher values show that this
///     scan is using a larger proportion of the I/O capacity of the system. Lower values
///     show that either this thread is not I/O bound or that it is getting a small share
///     of the I/O capacity of the system because of other concurrently executing
///     queries.
///
///   Hdfs Read Thread Concurrency Bucket - the bucket counting (%) of HDFS read thread
///     concurrency.
///
/// TODO: Once the legacy scan node has been removed, several functions can be made
/// non-virtual. Also merge this class with HdfsScanNodeMt.

class HdfsScanNodeBase : public ScanNode {
 public:
  HdfsScanNodeBase(ObjectPool* pool, const HdfsScanPlanNode& pnode,
      const THdfsScanNode& hdfs_scan_node, const DescriptorTbl& descs);
  ~HdfsScanNodeBase();

  virtual Status Prepare(RuntimeState* state) override WARN_UNUSED_RESULT;
  virtual Status Open(RuntimeState* state) override WARN_UNUSED_RESULT;
  virtual Status Reset(
      RuntimeState* state, RowBatch* row_batch) override WARN_UNUSED_RESULT;
  virtual void Close(RuntimeState* state) override;

  /// Returns true if this node uses separate threads for scanners that append RowBatches
  /// to a queue, false otherwise.
  virtual bool HasRowBatchQueue() const = 0;

  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  const std::vector<SlotDescriptor*>& virtual_column_slots() const {
      return virtual_column_slots_;
  }

  /// Returns number of partition keys in the table.
  int num_partition_keys() const { return hdfs_table_->num_clustering_cols(); }

  /// Returns number of partition key slots.
  int num_materialized_partition_keys() const { return partition_key_slots_.size(); }

  int stats_tuple_id() const { return stats_tuple_id_; }

  const std::vector<ScalarExprEvaluator*>& stats_conjunct_evals() const {
    return stats_conjunct_evals_;
  }

  const TupleDescriptor* stats_tuple_desc() const { return stats_tuple_desc_; }
  const TupleDescriptor* tuple_desc() const { return tuple_desc_; }
  const HdfsTableDescriptor* hdfs_table() const { return hdfs_table_; }
  const AvroSchemaElement& avro_schema() const { return avro_schema_; }
  int skip_header_line_count() const { return skip_header_line_count_; }
  io::RequestContext* reader_context() const { return reader_context_.get(); }
  bool optimize_count_star() const { return count_star_slot_offset_ != -1; }
  int count_star_slot_offset() const { return count_star_slot_offset_; }
  bool is_partition_key_scan() const { return is_partition_key_scan_; }

  typedef std::unordered_map<TupleId, std::vector<ScalarExprEvaluator*>>
    ConjunctEvaluatorsMap;
  const ConjunctEvaluatorsMap& conjuncts_map() const { return conjunct_evals_map_; }

  /// Slot Id => Dictionary Filter eligible conjuncts for that slot
  typedef std::map<TSlotId, std::vector<int32_t>> TDictFilterConjunctsMap;
  const TDictFilterConjunctsMap* thrift_dict_filter_conjuncts_map() const {
    return thrift_dict_filter_conjuncts_map_;
  }

  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length() {
    return max_compressed_text_file_length_;
  }
  RuntimeProfile::Counter* scanner_io_wait_time() const {
    return scanner_io_wait_time_;
  }

  const static int SKIP_COLUMN = -1;

  /// Returns index into materialized_slots with 'path'.  Returns SKIP_COLUMN if
  /// that path is not materialized.
  int GetMaterializedSlotIdx(const std::vector<int>& path) const {
    return static_cast<const HdfsScanPlanNode&>(plan_node_).GetMaterializedSlotIdx(path);
  }

  /// The result array is of length hdfs_table_->num_cols(). The i-th element is true iff
  /// column i should be materialized.
  const bool* is_materialized_col() {
    return reinterpret_cast<const bool*>(is_materialized_col_.data());
  }

  /// Returns a pointer to the atomic wrapper of the per format codegen'd function.
  /// Scanners call this to get the codegen'd function to use. Returns NULL if codegen
  /// should not be used.
  const CodegenFnPtrBase* GetCodegenFn(THdfsFileFormat::type);

  inline void IncNumScannersCodegenEnabled() { num_scanners_codegen_enabled_.Add(1); }
  inline void IncNumScannersCodegenDisabled() { num_scanners_codegen_disabled_.Add(1); }

  /// Allocate a new scan range object, stored in the fragment state's object pool. For
  /// scan ranges that correspond to the original hdfs splits, the partition id must be
  /// set to the range's partition id. Partition_id is mandatory as it is used to gather
  /// file descriptor info. expected_local should be true if this scan range is not
  /// expected to require a remote read. The range must fall within the file bounds.
  /// That is, the offset must be >= 0, and offset + len <= file_length.
  /// If not NULL, the 'original_split' pointer is stored for reference in the scan range
  /// metadata of the scan range that is to be allocated.
  /// This is thread safe.
  io::ScanRange* AllocateScanRange(const io::ScanRange::FileInfo &fi, int64_t len,
      int64_t offset, int64_t partition_id, int disk_id, bool expected_local,
      const io::BufferOpts& buffer_opts, const io::ScanRange* original_split = nullptr);

  /// Same as the first overload, but it takes sub-ranges as well.
  io::ScanRange* AllocateScanRange(const io::ScanRange::FileInfo &fi, int64_t len,
      int64_t offset, std::vector<io::ScanRange::SubRange>&& sub_ranges,
      int64_t partition_id, int disk_id, bool expected_local,
      const io::BufferOpts& buffer_opts, const io::ScanRange* original_split = nullptr);

  /// Same as above, but it takes both sub-ranges and metadata.
  io::ScanRange* AllocateScanRange(const io::ScanRange::FileInfo &fi, int64_t len,
      int64_t offset, std::vector<io::ScanRange::SubRange>&& sub_ranges,
      ScanRangeMetadata* metadata, int disk_id, bool expected_local,
      const io::BufferOpts& buffer_opts);

  /// Adds ranges to be read later by scanners. Must not be called once
  /// remaining_scan_range_submissions_ is 0. The enqueue_location specifies whether the
  /// scan ranges are added to the head or tail of the queue. Implemented by child classes
  /// to add ranges to their implementation of a queue that is used to organize ranges.
  virtual Status AddDiskIoRanges(const std::vector<io::ScanRange*>& ranges,
      EnqueueLocation enqueue_location = EnqueueLocation::TAIL) = 0;

  /// Adds all splits for file_desc to be read later by scanners.
  inline Status AddDiskIoRanges(const HdfsFileDesc* file_desc,
      EnqueueLocation enqueue_location = EnqueueLocation::TAIL) WARN_UNUSED_RESULT {
    return AddDiskIoRanges(file_desc->splits);
  }

  /// When this counter goes to 0, AddDiskIoRanges() can no longer be called.
  /// Furthermore, this implies that scanner threads failing to
  /// acquire a new scan range with StartNextScanRange() can exit.
  inline void UpdateRemainingScanRangeSubmissions(int32_t delta) {
    shared_state_->UpdateRemainingScanRangeSubmissions(delta);
  }

  /// Given a partition_id and filename returns the related file descriptor
  /// DCHECK ensures there is always file descriptor returned
  inline const HdfsFileDesc* GetFileDesc(
      int64_t partition_id, const std::string& filename) {
    return shared_state_->GetFileDesc(partition_id, filename);
  }

  /// Sets the scanner specific metadata for 'partition_id' and 'filename'.
  /// Scanners can use this to store file header information. Thread safe.
  inline void SetFileMetadata(
      int64_t partition_id, const std::string& filename, void* metadata) {
    shared_state_->SetFileMetadata(partition_id, filename, metadata);
  }

  /// Returns the scanner specific metadata for 'partition_id' and 'filename'.
  /// Returns nullptr if there is no metadata. Thread safe.
  inline void* GetFileMetadata(int64_t partition_id, const std::string& filename) {
    return shared_state_->GetFileMetadata(partition_id, filename);
  }

  /// Called by scanners when a range is complete. Used to record progress.
  /// This *must* only be called after a scanner has completely finished its
  /// scan range (i.e. context->Flush()), and has returned the final row batch.
  /// Otherwise, scan nodes using a RowBatch queue may lose the last batch due
  /// to racing with shutting down the queue.
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const THdfsCompression::type& compression_type, bool skipped = false);

  /// Same as above except for when multiple compression codecs were used
  /// in the file. The metrics are incremented for each compression_type.
  /// 'skipped' is set to true in the following cases -
  /// 1. when a scan range is filtered at runtime
  /// 2. scan range is a metadata read only(e.x. count(*) on parquet files)
  virtual void RangeComplete(const THdfsFileFormat::type& file_type,
      const std::vector<THdfsCompression::type>& compression_type, bool skipped = false);

  /// Calls RangeComplete() with skipped=true for all the splits of the file
  void SkipFile(const THdfsFileFormat::type& file_type, const HdfsFileDesc* file);

  /// Returns true if there are no materialized slots, such as a count(*) over the table.
  inline bool IsZeroSlotTableScan() const {
    return materialized_slots().empty() && tuple_desc()->tuple_path().empty() &&
        virtual_column_slots().empty();
  }

  /// Transfers all memory from 'pool' to shared state of all scanners.
  void TransferToSharedStatePool(MemPool* pool);

  /// map from volume id to <number of split, per volume split lengths>
  typedef boost::unordered_map<int32_t, std::pair<int, int64_t>> PerVolumeStats;

  /// Update the per volume stats with the given scan range params list
  static void UpdateHdfsSplitStats(
      const google::protobuf::RepeatedPtrField<ScanRangeParamsPB>& scan_range_params_list,
      PerVolumeStats* per_volume_stats);

  /// Output the per_volume_stats to stringstream. The output format is a list of:
  /// <volume id>:<# splits>/<per volume split lengths>
  static void PrintHdfsSplitStats(const PerVolumeStats& per_volume_stats,
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

  /// Returns true if Iceberg partition passes all the filter predicates in 'filter_ctxs'
  /// and should not be filtered out. Iceberg partition information is in the 'file'
  /// object. 'partition_id' is used to get the template tuple. 'stats_name' is the key of
  /// one of the counter groups in FilterStats, and is used to update the correct
  /// statistics. 'state' is used for logging.
  ///
  /// 'filter_ctxs' is either an empty list, in which case filtering is disabled and the
  /// function returns true, or a set of filter contexts to evaluate.
  bool IcebergPartitionPassesFilters(int64_t partition_id, const std::string& stats_name,
      const std::vector<FilterContext>& filter_ctxs, HdfsFileDesc* file,
      RuntimeState* state);

  /// Update book-keeping to skip the scan range if it has been issued but will not be
  /// processed by a scanner. E.g. used to cancel ranges that are filtered out by
  /// late-arriving filters that could not be applied in IssueInitialScanRanges()
  /// The ScanRange must have been added to a RequestContext.
  void SkipScanRange(io::ScanRange* scan_range);

  /// Helper to increase reservation from 'curr_reservation' up to 'ideal_reservation'
  /// that may succeed in getting a partial increase if the full increase is not
  /// possible. First increases to an I/O buffer multiple then increases in I/O buffer
  /// sized increments. 'curr_reservation' can refer to a "share' of the total
  /// reservation of the buffer pool client, e.g. the 'share" belonging to a single
  /// scanner thread. Returns the new reservation after increases.
  int64_t IncreaseReservationIncrementally(int64_t curr_reservation,
      int64_t ideal_reservation);

  /// Update the number of [un]compressed bytes read for the given SlotId. This is used
  /// to track the number of bytes read per column and is meant to be called by
  /// individual scanner classes.
  void UpdateBytesRead(
      SlotId slot_id, int64_t uncompressed_bytes_read, int64_t compressed_bytes_read);

  /// Get the template tuple which has only the partition columns materialized for the
  /// partition identified by 'partition_id'.
  inline Tuple* GetTemplateTupleForPartitionId(int64_t partition_id) {
    return shared_state_->GetTemplateTupleForPartitionId(partition_id);
  }

 protected:
  friend class ScannerContext;
  friend class HdfsScanner;

  /// Tuple id of the tuple used to evaluate conjuncts on parquet::Statistics.
  const int stats_tuple_id_;

  /// Conjuncts to evaluate on parquet::Statistics.
  const vector<ScalarExpr*>& stats_conjuncts_;
  vector<ScalarExprEvaluator*> stats_conjunct_evals_;

  /// Descriptor for the tuple used to evaluate conjuncts on parquet::Statistics.
  TupleDescriptor* stats_tuple_desc_ = nullptr;

  // Number of header lines to skip at the beginning of each file of this table. Only set
  // to values > 0 for hdfs text files.
  const int skip_header_line_count_;

  /// Tuple id of the tuple descriptor to be used.
  const int tuple_id_;

  /// The byte offset of the slot for Parquet/ORC metadata if count star optimization
  /// is enabled. When set, this scan node can optimize a count(*) query by populating
  /// the tuple with data from the Parquet num rows statistic. See
  /// applyCountStarOptimization() in ScanNode.java.
  const int count_star_slot_offset_;

  // True if this is a partition key scan that needs only to return at least one row from
  // each scan range. If true, the scan node and scanner implementations should attempt
  // to do the minimum possible work to materialise one row.
  const bool is_partition_key_scan_;

  /// RequestContext object to use with the disk-io-mgr for reads.
  std::unique_ptr<io::RequestContext> reader_context_;

  /// Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Descriptor for the hdfs table, including partition and format metadata.
  /// Set in Prepare, owned by RuntimeState
  const HdfsTableDescriptor* hdfs_table_ = nullptr;

  /// The root of the table's Avro schema, if we're scanning an Avro table.
  const AvroSchemaElement& avro_schema_;

  /// Conjuncts for each materialized tuple (top-level row batch tuples and collection
  /// item tuples). Includes a copy of ExecNode.conjuncts_.
  typedef std::unordered_map<TupleId, std::vector<ScalarExpr*>> ConjunctsMap;
  const ConjunctsMap& conjuncts_map_;
  ConjunctEvaluatorsMap conjunct_evals_map_;

  /// Dictionary filtering eligible conjuncts for each slot. Set to nullptr when there
  /// are no dictionary filters.
  const TDictFilterConjunctsMap* thrift_dict_filter_conjuncts_map_;

  /// Set to true when the initial scan ranges are issued to the IoMgr. This happens on
  /// the first call to GetNext() for non-MT scan nodes and in Open() for MT scan nodes.
  /// Only used by the thread token manager in non-MT scan nodes in a separate thread.
  AtomicBool initial_ranges_issued_;

  /// Per scanner type codegen'd fn.
  /// Owned by HdfsScanPlanNode.
  const HdfsScanPlanNode::CodegendFnMap& codegend_fn_map_;

  /// is_materialized_col_[i] = <true i-th column should be materialized, false otherwise>
  /// for 0 <= i < total # columns in table
  //
  /// This should be a vector<bool>, but bool vectors are special-cased and not stored
  /// internally as arrays, so instead we store as chars and cast to bools as needed
  const std::vector<char>& is_materialized_col_;

  /// Vector containing slot descriptors for all non-partition key slots.  These
  /// descriptors are sorted in order of increasing col_pos.
  const std::vector<SlotDescriptor*>& materialized_slots_;

  /// Vector containing slot descriptors for all partition key slots.
  const std::vector<SlotDescriptor*>& partition_key_slots_;

  const std::vector<SlotDescriptor*>& virtual_column_slots_;

  /// Counters which track the number of scanners that have codegen enabled for the
  /// materialize and conjuncts evaluation code paths.
  AtomicInt32 num_scanners_codegen_enabled_;
  AtomicInt32 num_scanners_codegen_disabled_;

  /// If true, counters are actively running and need to be reported in the runtime
  /// profile.
  bool counters_running_ = false;

  /// Disk accessed bitmap
  RuntimeProfile::Counter disks_accessed_bitmap_;
  /// The number of active hdfs reading threads reading for this node.
  RuntimeProfile::Counter active_hdfs_read_thread_counter_;

  /// Definition of following counters can be found in hdfs-scan-node-base.cc
  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length_ = nullptr;
  RuntimeProfile::Counter* bytes_read_local_ = nullptr;
  RuntimeProfile::Counter* bytes_read_short_circuit_ = nullptr;
  RuntimeProfile::Counter* bytes_read_dn_cache_ = nullptr;
  RuntimeProfile::Counter* bytes_read_encrypted_ = nullptr;
  RuntimeProfile::Counter* bytes_read_ec_ = nullptr;
  RuntimeProfile::Counter* num_remote_ranges_ = nullptr;
  RuntimeProfile::Counter* unexpected_remote_bytes_ = nullptr;
  RuntimeProfile::Counter* cached_file_handles_hit_count_ = nullptr;
  RuntimeProfile::Counter* cached_file_handles_miss_count_ = nullptr;
  RuntimeProfile::Counter* data_cache_hit_count_ = nullptr;
  RuntimeProfile::Counter* data_cache_partial_hit_count_ = nullptr;
  RuntimeProfile::Counter* data_cache_miss_count_ = nullptr;
  RuntimeProfile::Counter* data_cache_hit_bytes_ = nullptr;
  RuntimeProfile::Counter* data_cache_miss_bytes_ = nullptr;
  RuntimeProfile::Counter* scanner_io_wait_time_ = nullptr;
  RuntimeProfile::Counter* average_hdfs_read_thread_concurrency_ = nullptr;
  RuntimeProfile::Counter* per_read_thread_throughput_counter_ = nullptr;
  RuntimeProfile::Counter* num_disks_accessed_counter_ = nullptr;
  RuntimeProfile::Counter* hdfs_read_timer_ = nullptr;
  RuntimeProfile::Counter* hdfs_open_file_timer_ = nullptr;
  RuntimeProfile::SummaryStatsCounter* initial_range_ideal_reservation_stats_ = nullptr;
  RuntimeProfile::SummaryStatsCounter* initial_range_actual_reservation_stats_ = nullptr;
  RuntimeProfile::SummaryStatsCounter* compressed_bytes_read_per_column_counter_ =
      nullptr;
  RuntimeProfile::SummaryStatsCounter* uncompressed_bytes_read_per_column_counter_ =
      nullptr;

  /// HDFS read thread concurrency bucket: bucket[i] refers to the number of sample
  /// taken where there are i concurrent hdfs read thread running. Created in Open().
  std::vector<RuntimeProfile::Counter*>* hdfs_read_thread_concurrency_bucket_ = nullptr;

  /// Pool for allocating memory for Iceberg partition filtering.
  boost::scoped_ptr<MemPool> iceberg_partition_filtering_pool_;

  /// Status of failed operations.  This is set in the ScannerThreads
  /// Returned in GetNext() if an error occurred.  An non-ok status triggers cleanup
  /// scanner threads.
  Status status_;

  /// Struct that tracks the uncompressed and compressed bytes read. Used by the map
  /// bytes_read_per_col_ to track the [un]compressed bytes read per column. Types are
  /// atomic as the struct may be updated concurrently.
  struct BytesRead {
    AtomicInt64 uncompressed_bytes_read;
    AtomicInt64 compressed_bytes_read;
  };

  /// Map from SlotId (column identifer) to a pair where the first entry is the number of
  /// uncompressed bytes read for the column and the second entry is the number of
  /// compressed bytes read for the column. This map is used to update the
  /// [un]compressed_bytes_read_per_column counter.
  std::unordered_map<SlotId, BytesRead> bytes_read_per_col_;

  /// Lock that controls access to bytes_read_per_col_ so that multiple scanners
  /// can update the map concurrently
  boost::shared_mutex bytes_read_per_col_lock_;

  /// Pointer to the scan range related state that is shared across all node instances.
  ScanRangeSharedState* shared_state_ = nullptr;

  /// Utility class for handling file metadata.
  FileMetadataUtils file_metadata_utils_;

  /// Performs dynamic partition pruning, i.e., applies runtime filters to files, and
  /// issues initial ranges for all file types. Waits for runtime filters if necessary.
  /// Only valid to call if !initial_ranges_issued_. Sets initial_ranges_issued_ to true.
  /// A non-ok status is returned only if it encounters an invalid scan range or if a
  /// scanner thread cancels the scan when it runs into an error.
  Status IssueInitialScanRanges(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Gets the next scan range to process and allocates buffer for it. 'reservation' is
  /// an in/out argument with the current reservation available for this range. It may
  /// be increased by this function up to a computed "ideal" reservation, in which case
  /// *reservation is increased to reflect the new reservation.
  ///
  /// Scan ranges are checked against 'filter_ctxs' and scan ranges belonging to
  /// partitions that do not pass partition filters are filtered out.
  ///
  /// Returns Status::OK() and sets 'scan_range' if it gets a range to process. Returns
  /// Status::OK() and sets 'scan_range' to NULL when no more ranges are left to process.
  /// Returns an error status if there was an error getting the range or allocating
  /// buffers.
  Status StartNextScanRange(const std::vector<FilterContext>& filter_ctxs,
      int64_t* reservation, io::ScanRange** scan_range);

  /// Helper for the CreateAndOpenScanner() implementations in the subclass. Creates and
  /// opens a new scanner for this partition type. Depending on the outcome, the
  /// behaviour differs:
  /// - If the scanner is successfully created and opened, returns OK and sets *scanner.
  /// - If the scanner cannot be created, returns an error and does not set *scanner.
  /// - If the scanner is created but opening fails, returns an error and sets *scanner.
  ///   The caller is then responsible for closing the scanner.
  Status CreateAndOpenScannerHelper(HdfsPartitionDescriptor* partition,
      ScannerContext* context, boost::scoped_ptr<HdfsScanner>* scanner)
      WARN_UNUSED_RESULT;

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
  /// and therefore not processed. Returns true if all filters pass or are not present.
  bool FilePassesFilterPredicates(RuntimeState* state, HdfsFileDesc* file,
      const std::vector<FilterContext>& filter_ctxs);

  /// Stops periodic counters and aggregates counter values for the entire scan node.
  /// This should be called as soon as the scan node is complete to get the most accurate
  /// counter values.
  /// This can be called multiple times, subsequent calls will be ignored.
  /// This must be called on Close() to unregister counters.
  /// Scan nodes with a RowBatch queue may have to synchronize calls to this function.
  void StopAndFinalizeCounters();

  /// Calls ExecNode::ExecDebugAction() with 'phase'. Returns the status based on the
  /// debug action specified for the query.
  Status ScanNodeDebugAction(TExecNodePhase::type phase) WARN_UNUSED_RESULT;

  /// Fetches the next range in queue to read. Implemented by child classes to fetch
  /// ranges from their implementation of a queue that is used to organize ranges.
  virtual Status GetNextScanRangeToRead(
      io::ScanRange** scan_range, bool* needs_buffers) = 0;

 private:
  class HdfsCompressionTypesSet {
   public:
    HdfsCompressionTypesSet(): bit_map_(0) {
      DCHECK_GE(sizeof(bit_map_) * CHAR_BIT, _THdfsCompression_VALUES_TO_NAMES.size());
    }

    bool HasType(THdfsCompression::type type) {
      return (bit_map_ & (1 << type)) != 0;
    }

    void AddType(const THdfsCompression::type type) {
      bit_map_ |= (1 << type);
    }

    int Size() { return BitUtil::Popcount(bit_map_); }

    THdfsCompression::type GetFirstType() {
      DCHECK_GT(Size(), 0);
      for (auto& elem : _THdfsCompression_VALUES_TO_NAMES) {
        THdfsCompression::type type = static_cast<THdfsCompression::type>(elem.first);
        if (HasType(type))  return type;
      }
      return THdfsCompression::NONE;
    }

    // The following operator overloading is needed so this class can be part of the
    // std::map key.
    bool operator< (const HdfsCompressionTypesSet& o) const {
      return bit_map_ < o.bit_map_;
    }

    bool operator== (const HdfsCompressionTypesSet& o) const {
      return bit_map_ == o.bit_map_;
    }

   private:
    uint32_t bit_map_;
  };

  /// Mapping of file formats to the number of splits of that type. The key is a tuple
  /// containing:
  /// * file type
  /// * whether the split was skipped
  /// * compression types set
  typedef std::map<std::tuple<THdfsFileFormat::type, bool, HdfsCompressionTypesSet>, int>
      FileTypeCountsMap;
  FileTypeCountsMap file_type_counts_;
};
}

#endif

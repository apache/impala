// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>

#include <boost/unordered_set.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "exec/scan-node.h"
#include "exec/scanner-context.h"
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/string-buffer.h"
#include "util/progress-updater.h"
#include "util/thread.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class DescriptorTbl;
class HdfsScanner;
class RowBatch;
class Status;
class Tuple;
class TPlanNode;
class TScanRange;

// Maintains per file information for files assigned to this scan node.  This includes
// all the splits for the file as well as a lock which can be used when updating
// the file's metadata.
struct HdfsFileDesc {
  boost::mutex lock;
  std::string filename;

  // length of the file. This is not related to which parts of the file have been
  // assigned to this node.
  int64_t file_length;

  // Splits (i.e. raw byte ranges) for this file, assigned to this scan node.
  std::vector<DiskIoMgr::ScanRange*> splits;
  HdfsFileDesc(const std::string& filename) : filename(filename) {}
};

// Struct for additional metadata for scan ranges. This contains the partition id
// that this scan range is for.
struct ScanRangeMetadata {
  // The partition id that this range is part of.
  int64_t partition_id;

  ScanRangeMetadata(int64_t partition_id)
    : partition_id(partition_id) { }
};

// A ScanNode implementation that is used for all tables read directly from
// HDFS-serialised data.
// A HdfsScanNode spawns multiple scanner threads to process the bytes in
// parallel.  There is a handshake between the scan node and the scanners
// to get all the splits queued and bytes processed.
// 1. The scan node initially calls the Scanner with a list of files and splits
//    for that scanner/file format.
// 2. The scanner issues the initial byte ranges for each of those files.  For text
//    this is simply the entire range but for rc files, this would just be the header
//    byte range.  The scan node doesn't care either way.
// 3. The scan node spins up a number of scanner threads. Each of those threads
//    pulls the next scan range to work on from the IoMgr and then processes the
//    range end to end.
// 4. The scanner processes the buffers, issuing more scan ranges if necessary.
// 5. The scanner finishes the scan range and informs the scan node so it can track
//    end of stream.
// TODO: this class allocates a bunch of small utility objects that should be
// recycled.
class HdfsScanNode : public ScanNode {
 public:
  HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~HdfsScanNode();

  // ExecNode methods
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

  // ScanNode methods
  virtual Status SetScanRanges(const std::vector<TScanRangeParams>& scan_ranges);

  // Methods for the scanners to use
  Status CreateConjuncts(std::vector<Expr*>* exprs, bool disable_codegen);

  int limit() const { return limit_; }

  bool compact_data() const { return compact_data_; }

  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  // Returns the tuple idx into the row for this scan node to output to.
  // Currently this is always 0.
  int tuple_idx() const { return 0; }

  // Returns number of partition keys in the schema, including non-materialized slots
  int num_partition_keys() const { return num_partition_keys_; }

  // Returns number of materialized partition key slots
  int num_materialized_partition_keys() const { return partition_key_slots_.size(); }

  int num_cols() const { return column_idx_to_materialized_slot_idx_.size(); }

  const TupleDescriptor* tuple_desc() { return tuple_desc_; }

  const HdfsTableDescriptor* hdfs_table() { return hdfs_table_; }

  hdfsFS hdfs_connection() { return hdfs_connection_; }

  RuntimeState* runtime_state() { return runtime_state_; }

  DiskIoMgr::ReaderContext* reader_context() { return reader_context_; }

  const static int SKIP_COLUMN = -1;

  // Returns index into materialized_slots with 'col_idx'.  Returns SKIP_COLUMN if
  // that column is not materialized.
  int GetMaterializedSlotIdx(int col_idx) const {
    return column_idx_to_materialized_slot_idx_[col_idx];
  }

  // Returns the per format codegen'd function.  Scanners call this to get the
  // codegen function to use.  Returns NULL if codegen should not be used.
  llvm::Function* GetCodegenFn(THdfsFileFormat::type);

  // Each call to GetCodegenFn must call ReleaseCodegenFn.
  void ReleaseCodegenFn(THdfsFileFormat::type type, llvm::Function* fn);

  inline void IncNumScannersCodegenEnabled() {
    ++num_scanners_codegen_enabled_;
  }

  inline void IncNumScannersCodegenDisabled() {
    ++num_scanners_codegen_disabled_;
  }

  // Adds a materialized row batch for the scan node.  This is called from scanner
  // threads.
  // This function will block if materialized_row_batches_ is full.
  void AddMaterializedRowBatch(RowBatch* row_batch);

  // Allocate a new scan range object, stored in the runtime state's object pool.
  // This is thread safe.
  DiskIoMgr::ScanRange* AllocateScanRange(const char* file, int64_t len, int64_t offset,
      int64_t partition_id, int disk_id);

  // Adds ranges to the io mgr queue and starts up new scanner threads if possible.
  Status AddDiskIoRanges(const std::vector<DiskIoMgr::ScanRange*>& ranges);

  // Adds all splits for file_desc to the io mgr queue.
  Status AddDiskIoRanges(const HdfsFileDesc* file_desc);

  // Indicates that this file_desc's scan ranges have all been issued to the IoMgr.
  // For each file, the scanner must call MarkFileDescIssued() or AddDiskIoRanges().
  // Issuing ranges happens asynchronously. For many of the file formats we synchronously
  // issue the file header/footer in Open() but the rest of the splits for the file are
  // issued asynchronously.
  void MarkFileDescIssued(const HdfsFileDesc* file_desc);

  // Allocates and initialises template_tuple_ with any values from
  // the partition columns for the current scan range
  // Returns NULL if there are no materialized partition keys.
  // TODO: cache the tuple template in the partition object.
  Tuple* InitTemplateTuple(RuntimeState* state, const std::vector<Expr*>& expr_values);

  // Allocates and return an empty template tuple (i.e. with no values filled in).
  // Scanners can use this method to initialize a template tuple even if there are no
  // materialized partition keys (e.g. to hold Avro default values).
  Tuple* InitEmptyTemplateTuple();

  // Acquires all allocations from pool into scan_node_pool_. Thread-safe.
  void TransferToScanNodePool(MemPool* pool);

  // Returns the file desc for 'filename'.  Returns NULL if filename is invalid.
  HdfsFileDesc* GetFileDesc(const std::string& filename);

  // Gets scanner specific metadata for 'filename'.  Scanners can use this to store
  // file header information.
  // Returns NULL if there is no metadata.
  // This is thread safe.
  void* GetFileMetadata(const std::string& filename);

  // Sets the scanner specific metadata for 'filename'.
  // This is thread safe.
  void SetFileMetadata(const std::string& filename, void* metadata);

  // Called by the scanner when a range is complete.  Used to trigger done_ and
  // to log progress.  This *must* only be called after the scanner has completely
  // finished the scan range (i.e. context->Flush()).
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const THdfsCompression::type& compression_type);
  // Same as above except for when multiple compression codecs were used
  // in the file. The metrics are incremented for each compression_type.
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const std::vector<THdfsCompression::type>& compression_type);

  // Utility function to compute the order in which to materialize slots to allow  for
  // computing conjuncts as slots get materialized (on partial tuples).
  // 'order' will contain for each slot, the first conjunct it is associated with.
  // e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  // evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  // order set to conjuncts.size()
  void ComputeSlotMaterializationOrder(std::vector<int>* order) const;

  // map from volume id to <number of split, per volume split lengths>
  typedef boost::unordered_map<int32_t, std::pair<int, int64_t> > PerVolumnStats;

  // Update the per volume stats with the given scan range params list
  static void UpdateHdfsSplitStats(
      const std::vector<TScanRangeParams>& scan_range_params_list,
      PerVolumnStats* per_volume_stats);

  // Output the per_volume_stats to stringsteam. The output format is a list of:
  // <volume id>:<# splits>/<per volume split lengths>
  static void PrintHdfsSplitStats(const PerVolumnStats& per_volume_stats,
      std::stringstream* ss);

  // Description string for the per volume stats output.
  static const std::string HDFS_SPLIT_STATS_DESC;

 private:
  friend class ScannerContext;

  // Cache of the plan node.  This is needed to be able to create a copy of
  // the conjuncts per scanner since our Exprs are not thread safe.
  boost::scoped_ptr<TPlanNode> thrift_plan_node_;

  RuntimeState* runtime_state_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  const int tuple_id_;

  // Copy strings to tuple memory pool if true.
  // We try to avoid the overhead copying strings if the data will just
  // stream to another node that will release the memory.
  bool compact_data_;

  // ReaderContext object to use with the disk-io-mgr
  DiskIoMgr::ReaderContext* reader_context_;

  // Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_;

  // Descriptor for the hdfs table, including partition and format metadata.
  // Set in Prepare, owned by RuntimeState
  const HdfsTableDescriptor* hdfs_table_;

  // If true, the warning that some disk ids are unknown was logged.  Only log
  // this once per scan node since it can be noisy.
  bool unknown_disk_id_warned_;

  // Files and their splits
  typedef std::map<std::string, HdfsFileDesc*> SplitsMap;
  SplitsMap per_file_splits_;

  // Number of files that have not been issued from the scanners.
  AtomicInt<int> num_unqueued_files_;

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // Map of HdfsScanner objects to file types.  Only one scanner object will be
  // created for each file type.  Objects stored in runtime_state's pool.
  typedef std::map<THdfsFileFormat::type, HdfsScanner*> ScannerMap;
  ScannerMap scanner_map_;

  // Per scanner type codegen'd fn.  If the scan node only contains conjuncts that are
  // thread safe, there is only one entry in the list and the function is shared by all
  // scanners.  If the codegen'd fn is not thread safe, there is a number of these
  // functions that are shared by the scanner threads.  The number of entries in the list
  // is based on the maximum number of parallel scan ranges possible, which is in turn
  // based on the number of cpu cores.
  boost::mutex codgend_fn_map_lock_;
  typedef std::map<THdfsFileFormat::type, std::list<llvm::Function*> > CodegendFnMap;
  CodegendFnMap codegend_fn_map_;

  // Total number of partition slot descriptors, including non-materialized ones.
  int num_partition_keys_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots and partition key slots will
  // have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;

  // Vector containing slot descriptors for all materialized non-partition key
  // slots.  These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> materialized_slots_;

  // Vector containing slot descriptors for all materialized partition key slots
  // These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> partition_key_slots_;

  // Keeps track of total splits and the number finished.
  ProgressUpdater progress_;

  // Scanner specific per file metadata (e.g. header information) and associated lock.
  // This lock cannot be taken together with any other locks except lock_.
  boost::mutex metadata_lock_;
  std::map<std::string, void*> per_file_metadata_;

  // Thread group for all scanner worker threads
  ThreadGroup scanner_threads_;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  // Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  // This is the number of io buffers that are owned by the scan node and the scanners.
  // This is used just to help debug leaked io buffers to determine if the leak is
  // happening in the scanners vs other parts of the execution.
  AtomicInt<int> num_owned_io_buffers_;

  // The number of times a token was offered but no scanner threads started.
  // This is used for diagnostics only.
  AtomicInt<int> num_skipped_tokens_;

  // Counters which track the number of scanners that have codegen enabled for the
  // materialize and conjuncts evaluation code paths.
  AtomicInt<int> num_scanners_codegen_enabled_;
  AtomicInt<int> num_scanners_codegen_disabled_;

  // Disk accessed bitmap
  RuntimeProfile::Counter disks_accessed_bitmap_;

  // Total number of bytes read locally
  RuntimeProfile::Counter* bytes_read_local_;

  // Total number of bytes read via short circuit read
  RuntimeProfile::Counter* bytes_read_short_circuit_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and any other locks needs to be taken
  // together, this lock must be taken first.
  boost::mutex lock_;

  // Flag signaling that all scanner threads are done.  This could be because they
  // are finished, an error/cancellation occurred, or the limit was reached.
  // Setting this to true triggers the scanner threads to clean up.
  // This should not be explicitly set. Instead, call SetDone().
  bool done_;

  // Set to true if all ranges have started. Some of the ranges may still be in
  // flight being processed by scanner threads, but no new ScannerThreads
  // should be started.
  bool all_ranges_started_;

  // Pool for allocating some amounts of memory that is shared between scanners.
  // e.g. partition key tuple and their string buffers
  boost::scoped_ptr<MemPool> scan_node_pool_;

  // Status of failed operations.  This is set in the ScannerThreads
  // Returned in GetNext() if an error occurred.  An non-ok status triggers cleanup
  // scanner threads.
  Status status_;

  // Mapping of file formats (file type, compression type) to the number of
  // splits of that type and the lock protecting it.
  // This lock cannot be taken together with any other locks except lock_.
  boost::mutex file_type_counts_lock_;
  typedef std::map<
      std::pair<THdfsFileFormat::type, THdfsCompression::type>, int> FileTypeCountsMap;
  FileTypeCountsMap file_type_counts_;

  // If true, counters are actively running and need to be reported in the runtime
  // profile.
  bool counters_running_;

  // Called when scanner threads are available for this scan node. This will
  // try to spin up as many scanner threads as the quota allows.
  // This is also called whenever a new range is added to the IoMgr to 'pull'
  // thread tokens if they are available.
  void ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool);

  // Create a new scanner for this partition type.
  // If the scanner cannot be created return NULL.
  HdfsScanner* CreateScanner(HdfsPartitionDescriptor*);

  // Main function for scanner thread. This thread pulls the next range to be
  // processed from the IoMgr and then processes the entire range end to end.
  // This thread terminates when all scan ranges are complete or an error occurred.
  void ScannerThread();
  void ScannerThreadHelper();

  // Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // sets done_ to true and triggers threads to cleanup. Cannot be calld with
  // any locks taken. Calling it repeatedly ignores subsequent calls.
  void SetDone();

  // Stops periodic counters and aggregates counter values for the entire scan node.
  // This should be called as soon as the scan node is complete to get the most accurate
  // counter values.
  // This can be called multiple times, subsequent calls will be ignored.
  // This must be called on Close() to unregister counters.
  void StopAndFinalizeCounters();
};

}

#endif

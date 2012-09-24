// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>

#include <hdfs.h>

#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/string-buffer.h"
#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class ByteStream;
class DescriptorTbl;
class HdfsScanner;
class RowBatch;
class Status;
class ScanRangeContext;
class Tuple;
class TPlanNode;
class TScanRange;

// Maintains per file information for files assigned to this scan node.  This includes 
// all the scan ranges for the file as well as a lock which can be used when updating 
// the file's metadata.
struct HdfsFileDesc {
  boost::mutex lock;
  std::string filename;
  std::vector<DiskIoMgr::ScanRange*> ranges;
  HdfsFileDesc(const std::string& filename) : filename(filename) {}
};

// A ScanNode implementation that is used for all tables read directly from 
// HDFS-serialised data. 
// A HdfsScanNode spawns multiple scanner threads to process the bytes in
// parallel.  There is a handshake between the scan node and the scanners 
// to get all the scan ranges queued and bytes processed.  
// 1. The scan node initially calls the Scanner with a list of files and ranges 
//    for that scanner/file format.
// 2. The scanner issues the initial byte ranges for each of those files.  For text
//    this is simply the entire range but for rc files, this would just be the header
//    byte range.  The scan node doesn't care either way.
// 3. Buffers for the issued ranges return and the scan node enqueues them to the
//    scanner's ready buffer queue.  
// 4. The scanner processes the buffers, issuing more byte ranges if necessary.
// 5. The scanner finishes the scan range and informs the scan node so it can track
//    end of stream.
// TODO: this class currently throttles ranges sent to the io mgr.  This should be
// updated to not have to do this once we can restrict the number of scanner threads
// a better way.
// TODO: this needs to be moved into the io mgr.  RegisterReader needs to take
// another argument for max parallel ranges or something like that.
// TODO: this class supports two types of scanners, TEXT and SEQUENCE which use
// the io mgr and RCFILE and TREVNI which don't.  Remove the non io mgr path.
class HdfsScanNode : public ScanNode {
 public:
  HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~HdfsScanNode();

  // ExecNode methods

  virtual Status Prepare(RuntimeState* state);

  virtual Status Open(RuntimeState* state);

  // Checks for cancellation at the very beginning and then again after
  // each call to HdfsScanner::GetNext().
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  virtual Status Close(RuntimeState* state);

  // ScanNode methods

  virtual Status SetScanRange(const TScanRange& scan_range);

  // Methods for the scanners to use

  Status CreateConjuncts(std::vector<Expr*>* exprs);

  int limit() const { return limit_; }

  bool compact_data() const { return compact_data_; }
  
  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  int num_partition_keys() const { return num_partition_keys_; }

  int num_cols() const { return column_idx_to_materialized_slot_idx_.size(); }
  
  const TupleDescriptor* tuple_desc() { return tuple_desc_; }

  hdfsFS hdfs_connection() { return hdfs_connection_; }
 
  DiskIoMgr::ReaderContext* reader_context() { return reader_context_; }

  // Returns index into materialized_slots with 'col_idx'.  Returns SKIP_COLUMN if
  // that column is not materialized.
  int GetMaterializedSlotIdx(int col_idx) const {
    return column_idx_to_materialized_slot_idx_[col_idx];
  }

  // Adds a materialized row batch for the scan node.  This is called from scanner
  // threads.
  void AddMaterializedRowBatch(RowBatch* row_batch);

  // Allocate a new scan range object.  This is thread safe.
  DiskIoMgr::ScanRange* AllocateScanRange(const char* file, int64_t len, int64_t offset,
      int64_t partition_id, int disk_id);

  // Adds a scan range to the disk io mgr queue.
  void AddDiskIoRange(DiskIoMgr::ScanRange* range);

  // Adds all ranges for file_desc to the io mgr queue.
  void AddDiskIoRange(const HdfsFileDesc* file_desc);

  // Scanners must call this when an entire file is queued.
  void FileQueued(const char* filename);

  // Allocates and initialises template_tuple_ with any values from
  // the partition columns for the current scan range
  Tuple* InitTemplateTuple(RuntimeState* state, const std::vector<Expr*>& expr_values);

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

  // Called by the scanner when a range is complete.  Used to log progress.
  void RangeComplete();

  const static int SKIP_COLUMN = -1;

 private:
  // Cache of the plan node.  This is needed to be able to create a copy of
  // the conjuncts per scanner since our Exprs are not thread safe.
  boost::scoped_ptr<TPlanNode> thrift_plan_node_;

  RuntimeState* runtime_state_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  int tuple_id_;

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

  // Mem pool for tuple buffer data. Used by scanners for allocation,
  // but owned here.
  boost::scoped_ptr<MemPool> tuple_pool_;

  // Files and their scan ranges
  typedef std::map<std::string, HdfsFileDesc*> ScanRangeMap;
  ScanRangeMap per_file_scan_ranges_;
  
  // Points at the (file, [scan ranges]) pair currently being processed
  ScanRangeMap::iterator current_file_scan_ranges_;

  // The index of the current scan range in the current file's scan range list
  int current_range_idx_;

  // Number of files that have not been issued from the scanners.
  int num_unqueued_files_;

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // Map of HdfsScanner objects to file types.  Only one scanner object will be
  // created for each file type.  Objects stored in scanner_pool_.
  typedef std::map<THdfsFileFormat::type, HdfsScanner*> ScannerMap;
  ScannerMap scanner_map_;

  // Pool for storing allocated scanner objects.  We don't want to use the 
  // runtime pool to ensure that the scanner objects are deleted before this
  // object is.
  boost::scoped_ptr<ObjectPool> scanner_pool_;

  // The scanner in use for the current file / scan-range
  // combination.
  HdfsScanner* current_scanner_;

  // The source of byte data for consumption by the scanner for the
  // current file / scan-range combination.
  boost::scoped_ptr<ByteStream> current_byte_stream_;

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

  // Thread that constantly reads from the disk io mgr and queues the work on the
  // context for that scan range.
  boost::scoped_ptr<boost::thread> disk_read_thread_;
 
  // Number of scanner threads to use
  int num_scanner_threads_;

  // Total number of scan ranges assigned to this scan node.  Used to report progress.
  int total_scan_ranges_;     

  // The number of scan ranges that have finished.  Used to report progress.
  int num_ranges_finished_;

  // Scanner specific per file metadata (e.g. header information) and associated lock.
  boost::mutex metadata_lock_;
  std::map<std::string, void*> per_file_metadata_;

  // Thread group for all scanner worker threads
  boost::thread_group scanner_threads_;

  // Lock and condition variable protecting materialized_row_batches_.  Row batches
  // are produced by the scanner threads and consumed by the main thread in GetNext
  // Lock to protect materialized_row_batches_
  boost::mutex row_batches_lock_;
  boost::condition_variable row_batch_added_cv_;
  std::list<RowBatch*> materialized_row_batches_;

  // Flag signaling that all scanner threads are done.  This could be because they
  // are finished, an error/cancellation occurred, or the limit was reached.
  // Setting this to true triggers the scanner threads to clean up.
  bool done_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and one of the condition variable
  // locks needs to be take together, this lock must be taken first.
  // This lock is recursive since some of the functions provided to the scanner are
  // also called from internal functions.
  // TODO: we can split out 'external' functions for internal functions and make this
  // lock non-recursive.
  boost::recursive_mutex lock_;

  // Pool for allocating partition key tuple and string buffers
  boost::scoped_ptr<MemPool> partition_key_pool_;

  // The queue of all ranges that the scanners want to issue.
  std::vector<DiskIoMgr::ScanRange*> all_ranges_;

  // The next idx (in all_ranges_) that should be issued.
  int next_range_to_issue_idx_;

  // If true, all ranges have been queued by the scanners.  They haven't necessarily
  // made it to the io mgr yet though.
  bool all_ranges_in_queue_;
  
  // The number of ranges in flight in the io mgr.
  int ranges_in_flight_;

  // If true, all ranges have been sent to the io mgr.
  bool all_ranges_issued_;

  // Mapping of hdfs scan range to the scan range context for processing it.  
  typedef std::map<const DiskIoMgr::ScanRange*, ScanRangeContext*> ContextMap;
  ContextMap contexts_;

  // Status of failed operations.  This is set asynchronously in DiskThread and
  // ScannerThread.  Returned in GetNext() if an error occurred.  An non-ok
  // status triggers cleanup of the disk and scanner threads.
  Status status_;

  // Issue the next set of queued ranges to the io mgr.  This is used to throttle
  // the number of scan ranges being parsed to the number of scanner threads.
  Status IssueMoreRanges();
  
  // Called once per scan-range to initialise (potentially) a new byte
  // stream and to call the same method on the current scanner.
  Status InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished);

  // Gets a scanner for the file type for this partition, creating one if none
  // exists.
  // TODO: remove when all scanners are updated.
  HdfsScanner* GetScanner(HdfsPartitionDescriptor*);

  // Create a new scanner for this partition type and initialize it.
  HdfsScanner* CreateScanner(HdfsPartitionDescriptor*);

  // Main function for disk thread which reads from the io mgr and pushes read
  // buffers to the scan range context.  This thread spawns new scanner threads
  // for each new context.
  void DiskThread();

  // Start a new scanner thread for this range with the initial 'buffer'.
  void StartNewScannerThread(DiskIoMgr::BufferDescriptor* buffer);

  // Main function for scanner thread.  This simply delegates to the scanner
  // to process the range.  This thread terminates when the scan range is complete
  // or an error occurred.
  void ScannerThread(HdfsScanner* scanner, ScanRangeContext*);
};

}

#endif

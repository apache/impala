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


#ifndef IMPALA_EXEC_HDFS_SCANNER_H_
#define IMPALA_EXEC_HDFS_SCANNER_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>

#include "codegen/impala-ir.h"
#include "common/object-pool.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scan-node.h"
#include "exec/scanner-context.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/row-batch.h"

namespace impala {

class CollectionValueBuilder;
class Compression;
class DescriptorTbl;
class Expr;
class HdfsPartitionDescriptor;
class MemPool;
class SlotDescriptor;
class Status;
class TextConverter;
class Tuple;
class TupleDescriptor;
class TPlanNode;
class TScanRange;
class Codec;

/// Intermediate structure used for two pass parsing approach. In the first pass,
/// the FieldLocation structs are filled out and contain where all the fields start and
/// their lengths.  In the second pass, the FieldLocation is used to write out the
/// slots. We want to keep this struct as small as possible.
struct FieldLocation {
  /// start of field. This is set to NULL for FieldLocations that are past the
  /// end of the row in the file. E.g. the file only had 3 cols but the table
  /// has 10. These then get written out as NULL slots.
  char* start;
  /// Encodes the length and whether or not this fields needs to be unescaped.
  /// If len < 0, then the field needs to be unescaped.
  ///
  /// Currently, 'len' has to fit in a 32-bit integer as that's the limit for StringValue
  /// and StringVal. All other types shouldn't be anywhere near this limit.
  int len;

  static const char* LLVM_CLASS_NAME;
};

/// HdfsScanner is the superclass for different hdfs file format parsers.  There is
/// an instance of the scanner object created for each split, each driven by a different
/// thread created by the scan node.  The scan node calls:
/// 1. Open()
/// 2. ProcessSplit() or GetNext()*
/// 3. Close()
/// The scanner can be used in either of two modes, indicated via the add_batches_to_queue
/// c'tor parameter.
/// ProcessSplit() scans the split and adds materialized row batches to the scan node's
/// row batch queue until the split is complete or an error occurred.
/// GetNext() provides an iterator-like interface where the caller can request
/// the next materialized row batch until the split has been fully processed (eos).
///
/// The HdfsScanner works in tandem with the ScannerContext to interleave IO
/// and parsing.
//
/// If a split is compressed, then a decompressor will be created, either during Prepare()
/// or at the beginning of ProcessSplit(), and used for decompressing and reading the
/// split.
//
/// For codegen, the implementation is split into two parts.
/// 1. During the Prepare() phase of the ScanNode, the scanner subclass's static
///    Codegen() function will be called to perform codegen for that scanner type
///    for the specific tuple desc. This codegen'd function is cached in the HdfsScanNode.
/// 2. During the GetNext() phase of the scan node (where we create one Scanner for each
///    scan range), the created scanner subclass can retrieve, from the scan node,
///    the codegen'd function to use.
/// This way, we only codegen once per scanner type, rather than once per scanner object.
//
/// This class also encapsulates row batch management.  Subclasses should call CommitRows()
/// after writing to the current row batch, which handles creating row batches, attaching
/// resources (IO buffers and mem pools) to the current row batch, and passing row batches
/// up to the scan node. Subclasses can also use GetMemory() to help with per-row memory
/// management.
class HdfsScanner {
 public:
  /// Assumed size of an OS file block.  Used mostly when reading file format headers, etc.
  /// This probably ought to be a derived number from the environment.
  const static int FILE_BLOCK_SIZE = 4096;

  /// If 'add_batches_to_queue' is true the caller must call ProcessSplit() and not
  /// GetNext(). Row batches will be added to the scan node's row batch queue, including
  /// the final one in Close().
  HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state, bool add_batches_to_queue);

  virtual ~HdfsScanner();

  /// One-time initialisation of state that is constant across scan ranges.
  virtual Status Open(ScannerContext* context);

  /// Returns the next row batch from this scanner's split.
  /// Recoverable errors are logged to the runtime state. Only returns a non-OK status
  /// if a non-recoverable error is encountered (or abort_on_error is true). If OK is
  /// returned, 'parse_status_' is guaranteed to be OK as well.
  /// The memory referenced by the tuples is valid until this or any subsequently
  /// returned batch is reset or destroyed.
  /// Only valid to call if 'add_batches_to_queue_' is false.
  Status GetNext(RowBatch* row_batch, bool* eos) {
    DCHECK(!add_batches_to_queue_);
    return GetNextInternal(row_batch, eos);
  }

  /// Process an entire split, reading bytes from the context's streams.  Context is
  /// initialized with the split data (e.g. template tuple, partition descriptor, etc).
  /// This function should only return on error or end of scan range.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  virtual Status ProcessSplit() = 0;

  /// Transfers the ownership of memory backing returned tuples such as IO buffers
  /// and memory in mem pools to the given row batch. If the row batch is NULL,
  /// those resources are released instead. In any case, releases all other resources
  /// that are not backing returned rows (e.g. temporary decompression buffers).
  virtual void Close(RowBatch* row_batch);

  /// Only valid to call if 'add_batches_to_queue_' is true.
  RowBatch* batch() const {
    DCHECK(add_batches_to_queue_);
    return batch_;
  }

  /// Scanner subclasses must implement these static functions as well.  Unfortunately,
  /// c++ does not allow static virtual functions.

  /// Issue the initial ranges for 'files'.  HdfsFileDesc groups all the splits
  /// assigned to this scan node by file.  This is called before any of the scanner
  /// subclasses are created to process splits in 'files'.
  /// The strategy on how to parse the scan ranges depends on the file format.
  /// - For simple text files, all the splits are simply issued to the io mgr and
  /// one split == one scan range.
  /// - For formats with a header, the metadata is first parsed, and then the ranges are
  /// issued to the io mgr.  There is one scan range for the header and one range for
  /// each split.
  /// - For columnar formats, the header is parsed and only the relevant byte ranges
  /// should be issued to the io mgr.  This is one range for the metadata and one
  /// range for each column, for each split.
  /// This function is how scanners can pick their strategy.
  /// void IssueInitialRanges(HdfsScanNode* scan_node,
  ///                         const std::vector<HdfsFileDesc*>& files);

  /// Codegen all functions for this scanner.  The codegen'd function is specific to
  /// the scanner subclass but not specific to each scanner object.  We don't want to
  /// codegen the functions for each scanner object.
  /// llvm::Function* Codegen(HdfsScanNode* scan_node);

  static const char* LLVM_CLASS_NAME;

 protected:
  /// The scan node that started this scanner
  HdfsScanNode* scan_node_;

  /// RuntimeState for error reporting
  RuntimeState* state_;

  /// True if the creator of this scanner intends to use ProcessSplit() and not GetNext).
  /// Row batches will be added to the scan node's row batch queue, including the final
  /// one in Close().
  const bool add_batches_to_queue_;

  /// Context for this scanner
  ScannerContext* context_;

  /// Object pool for objects with same lifetime as scanner.
  ObjectPool obj_pool_;

  /// The first stream for context_
  ScannerContext::Stream* stream_;

  /// Clones of the conjuncts ExprContexts in scan_node_->conjuncts_map(). Each scanner
  /// has its own ExprContexts so the conjuncts can be safely evaluated in parallel.
  HdfsScanNode::ConjunctsMap scanner_conjuncts_map_;

  // Convenience reference to scanner_conjuncts_map_[scan_node_->tuple_idx()] for scanners
  // that do not support nested types.
  const std::vector<ExprContext*>* scanner_conjunct_ctxs_;

  /// A template tuple is a partially-materialized tuple with only partition key slots set
  /// (or other default values, such as NULL for columns missing in a file).  The other
  /// slots are set to NULL.  The template tuple must be copied into output tuples before
  /// any of the other slots are materialized.
  ///
  /// Each tuple descriptor (i.e. scan_node_->tuple_desc() and any collection item tuple
  /// descs) has a template tuple, or NULL if there are no partition key or default slots.
  /// Template tuples are computed once for each file and valid for the duration of that
  /// file.  They are owned by the HDFS scan node, although each scanner has its own
  /// template tuples.
  std::map<const TupleDescriptor*, Tuple*> template_tuple_map_;

  /// Convenience variable set to the top-level template tuple
  /// (i.e. template_tuple_map_[scan_node_->tuple_desc()]).
  Tuple* template_tuple_;

  /// Fixed size of each top-level tuple, in bytes
  int tuple_byte_size_;

  /// Current tuple pointer into tuple_mem_.
  Tuple* tuple_;

  /// The current row batch being populated. Creating new row batches, attaching context
  /// resources, and handing off to the scan node is handled by this class in CommitRows(),
  /// but AttachPool() must be called by scanner subclasses to attach any memory allocated
  /// by that subclass. All row batches created by this class are transferred to the scan
  /// node (i.e., all batches are ultimately owned by the scan node).
  RowBatch* batch_;

  /// The tuple memory of batch_.
  uint8_t* tuple_mem_;

  /// Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  /// Number of null bytes in the top-level tuple.
  int32_t num_null_bytes_;

  /// Contains current parse status to minimize the number of Status objects returned.
  /// This significantly minimizes the cross compile dependencies for llvm since status
  /// objects inline a bunch of string functions.  Also, status objects aren't extremely
  /// cheap to create and destroy.
  Status parse_status_;

  /// Decompressor class to use, if any.
  boost::scoped_ptr<Codec> decompressor_;

  /// The most recently used decompression type.
  THdfsCompression::type decompression_type_;

  /// Pool to allocate per data block memory.  This should be used with the
  /// decompressor and any other per data block allocations.
  boost::scoped_ptr<MemPool> data_buffer_pool_;

  /// Time spent decompressing bytes.
  RuntimeProfile::Counter* decompress_timer_;

  /// Matching typedef for WriteAlignedTuples for codegen.  Refer to comments for
  /// that function.
  typedef int (*WriteTuplesFn)(HdfsScanner*, MemPool*, TupleRow*, int, FieldLocation*,
      int, int, int, int);
  /// Jitted write tuples function pointer.  Null if codegen is disabled.
  WriteTuplesFn write_tuples_fn_;

  /// Implements GetNext(). Should be overridden by subclasses.
  /// May be called even if 'add_batches_to_queue_' is true.
  virtual Status GetNextInternal(RowBatch* row_batch, bool* eos) {
    DCHECK(false) << "GetNextInternal() not implemented for this scanner type.";
    return Status::OK();
  }

  /// Initializes write_tuples_fn_ to the jitted function if codegen is possible.
  /// - partition - partition descriptor for this scanner/scan range
  /// - type - type for this scanner
  /// - scanner_name - debug string name for this scanner (e.g. HdfsTextScanner)
  Status InitializeWriteTuplesFn(HdfsPartitionDescriptor* partition,
      THdfsFileFormat::type type, const std::string& scanner_name);

  /// Set batch_ to a new row batch and update tuple_mem_ accordingly.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  Status StartNewRowBatch();

  /// Reset internal state for a new scan range.
  virtual Status InitNewRange() = 0;

  /// Gets memory for outputting tuples into batch_.
  ///  *pool is the mem pool that should be used for memory allocated for those tuples.
  ///  *tuple_mem should be the location to output tuples, and
  ///  *tuple_row_mem for outputting tuple rows.
  /// Returns the maximum number of tuples/tuple rows that can be output (before the
  /// current row batch is complete and a new one is allocated).
  /// Memory returned from this call is invalidated after calling CommitRows().
  /// Callers must call GetMemory() again after calling this function.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  int GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem);

  /// Gets memory for outputting tuples into the CollectionValue being constructed via
  /// 'builder'. If memory limit is exceeded, an error status is returned. Otherwise,
  /// returns the maximum number of tuples that can be output in 'num_rows'.
  ///
  /// The returned TupleRow* should not be incremented (i.e. don't call next_row() on
  /// it). Instead, incrementing *tuple_mem will update *tuple_row_mem to be pointing at
  /// the next tuple. This also means its unnecessary to call
  /// (*tuple_row_mem)->SetTuple().
  Status GetCollectionMemory(CollectionValueBuilder* builder, MemPool** pool,
      Tuple** tuple_mem, TupleRow** tuple_row_mem, int64_t* num_rows);

  /// Commit num_rows to the current row batch.  If this completes, the row batch is
  /// enqueued with the scan node and StartNewRowBatch() is called.
  /// Returns Status::OK if the query is not cancelled and hasn't exceeded any mem limits.
  /// Scanner can call this with 0 rows to flush any pending resources (attached pools
  /// and io buffers) to minimize memory consumption.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  Status CommitRows(int num_rows);

  /// Release all memory in 'pool' to batch_. If commit_batch is true, the row batch
  /// will be committed. commit_batch should be true if the attached pool is expected
  /// to be non-trivial (i.e. a decompression buffer) to minimize scanner mem usage.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  void AttachPool(MemPool* pool, bool commit_batch) {
    DCHECK(add_batches_to_queue_);
    DCHECK(batch_ != NULL);
    DCHECK(pool != NULL);
    batch_->tuple_data_pool()->AcquireData(pool, false);
    if (commit_batch) CommitRows(0);
  }

  /// Convenience function for evaluating conjuncts using this scanner's ExprContexts.
  /// This must always be inlined so we can correctly replace the call to
  /// ExecNode::EvalConjuncts() during codegen.
  bool IR_ALWAYS_INLINE EvalConjuncts(TupleRow* row)  {
    return ExecNode::EvalConjuncts(&(*scanner_conjunct_ctxs_)[0],
                                   scanner_conjunct_ctxs_->size(), row);
  }

  /// Utility method to write out tuples when there are no materialized
  /// fields (e.g. select count(*) or only partition keys).
  ///   num_tuples - Total number of tuples to write out.
  /// Returns the number of tuples added to the row batch.
  int WriteEmptyTuples(RowBatch* row_batch, int num_tuples);

  /// Write empty tuples and commit them to the context object
  int WriteEmptyTuples(ScannerContext* context, TupleRow* tuple_row, int num_tuples);

  /// Processes batches of fields and writes them out to tuple_row_mem.
  /// - 'pool' mempool to allocate from for auxiliary tuple memory
  /// - 'tuple_row_mem' preallocated tuple_row memory this function must use.
  /// - 'fields' must start at the beginning of a tuple.
  /// - 'num_tuples' number of tuples to process
  /// - 'max_added_tuples' the maximum number of tuples that should be added to the batch.
  /// - 'row_start_index' is the number of rows that have already been processed
  ///   as part of WritePartialTuple.
  /// Returns the number of tuples added to the row batch.  This can be less than
  /// num_tuples/tuples_till_limit because of failed conjuncts.
  /// Returns -1 if parsing should be aborted due to parse errors.
  /// Only valid to call if 'add_batches_to_queue_' is true.
  int WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row_mem, int row_size,
      FieldLocation* fields, int num_tuples,
      int max_added_tuples, int slots_per_tuple, int row_start_indx);

  /// Update the decompressor_ object given a compression type or codec name. Depending on
  /// the old compression type and the new one, it may close the old decompressor and/or
  /// create a new one of different type.
  Status UpdateDecompressor(const THdfsCompression::type& compression);
  Status UpdateDecompressor(const std::string& codec);

  /// Utility function to report parse errors for each field.
  /// If errors[i] is nonzero, fields[i] had a parse error.
  /// row_idx is the idx of the row in the current batch that had the parse error
  /// Returns false if parsing should be aborted.  In this case parse_status_ is set
  /// to the error.
  /// This is called from WriteAlignedTuples.
  bool ReportTupleParseError(FieldLocation* fields, uint8_t* errors, int row_idx);

  /// Triggers debug action of the scan node. This is currently used by parquet column
  /// readers to exercise various failure paths in parquet scanner. Returns the status
  /// returned by the scan node's TriggerDebugAction().
  Status TriggerDebugAction() { return scan_node_->TriggerDebugAction(); }

  /// Utility function to append an error message for an invalid row.  This is called
  /// from ReportTupleParseError()
  /// row_idx is the index of the row in the current batch.  Subclasses should override
  /// this function (i.e. text needs to join boundary rows).  Since this is only in the
  /// error path, vtable overhead is acceptable.
  virtual void LogRowParseError(int row_idx, std::stringstream*);

  /// Writes out all slots for 'tuple' from 'fields'. 'fields' must be aligned
  /// to the start of the tuple (e.g. fields[0] maps to slots[0]).
  /// After writing the tuple, it will be evaluated against the conjuncts.
  ///  - error_fields is an out array.  error_fields[i] will be set to true if the ith
  ///    field had a parse error
  ///  - error_in_row is an out bool.  It is set to true if any field had parse errors
  /// Returns whether the resulting tuplerow passed the conjuncts.
  //
  /// The parsing of the fields and evaluating against conjuncts is combined in this
  /// function.  This is done so it can be possible to evaluate conjuncts as slots
  /// are materialized (on partial tuples).
  //
  /// This function is replaced by a codegen'd function at runtime.  This is
  /// the reason that the out error parameters are typed uint8_t instead of bool. We need
  /// to be able to match this function's signature identically for the codegen'd function.
  /// Bool's as out parameters can get converted to bytes by the compiler and rather than
  /// implicitly depending on that to happen, we will explicitly type them to bytes.
  /// TODO: revisit this
  bool WriteCompleteTuple(MemPool* pool, FieldLocation* fields, Tuple* tuple,
      TupleRow* tuple_row, Tuple* template_tuple, uint8_t* error_fields,
      uint8_t* error_in_row);

  /// Codegen function to replace WriteCompleteTuple. Should behave identically
  /// to WriteCompleteTuple.
  static llvm::Function* CodegenWriteCompleteTuple(HdfsScanNode*, LlvmCodeGen*,
      const std::vector<ExprContext*>& conjunct_ctxs);

  /// Codegen function to replace WriteAlignedTuples.  WriteAlignedTuples is cross compiled
  /// to IR.  This function loads the precompiled IR function, modifies it and returns the
  /// resulting function.
  static llvm::Function* CodegenWriteAlignedTuples(HdfsScanNode*, LlvmCodeGen*,
      llvm::Function* write_tuple_fn);

  /// Report parse error for column @ desc.   If abort_on_error is true, sets
  /// parse_status_ to the error message.
  void ReportColumnParseError(const SlotDescriptor* desc, const char* data, int len);

  /// Initialize a tuple.
  /// TODO: only copy over non-null slots.
  /// TODO: InitTuple is called frequently, avoid the if, perhaps via templatization.
  void InitTuple(const TupleDescriptor* desc, Tuple* template_tuple, Tuple* tuple) {
    if (template_tuple != NULL) {
      memcpy(tuple, template_tuple, desc->byte_size());
    } else {
      memset(tuple, 0, sizeof(uint8_t) * desc->num_null_bytes());
    }
  }

  // TODO: replace this function with above once we can inline constants from
  // scan_node_->tuple_desc() via codegen
  void InitTuple(Tuple* template_tuple, Tuple* tuple) {
    if (template_tuple != NULL) {
      memcpy(tuple, template_tuple, tuple_byte_size_);
    } else {
      memset(tuple, 0, sizeof(uint8_t) * num_null_bytes_);
    }
  }

  inline Tuple* next_tuple(int tuple_byte_size, Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + tuple_byte_size);
  }

  inline TupleRow* next_row(TupleRow* r) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(r);
    return reinterpret_cast<TupleRow*>(mem + batch_->row_byte_size());
  }

  /// Simple wrapper around scanner_conjunct_ctxs_. Used in the codegen'd version of
  /// WriteCompleteTuple() because it's easier than writing IR to access
  /// scanner_conjunct_ctxs_.
  ExprContext* GetConjunctCtx(int idx) const;

  /// Unit test constructor
  HdfsScanner();
};

}

#endif

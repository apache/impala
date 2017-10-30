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
#include <boost/scoped_ptr.hpp>

#include "codegen/impala-ir.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/scanner-context.h"
#include "runtime/row-batch.h"
#include "runtime/tuple.h"

namespace impala {

class Codec;
class CollectionValueBuilder;
class Compression;
class Expr;
class HdfsPartitionDescriptor;
class MemPool;
class TextConverter;
class TupleDescriptor;
class SlotDescriptor;

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
/// The scanner can be used in either of two modes. Which mode is expected to be used
/// depends on the type of parent scan node. Parent scan nodes with a row batch queue
/// are expected to call ProcessSplit() and not GetNext(). Row batches will be added to
/// the scan node's row batch queue, including the final one in Close().
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
/// TODO: Have a pass over all members and move them out of the base class if sensible
/// to clarify which state each concrete scanner type actually has.
class HdfsScanner {
 public:
  /// Assumed size of an OS file block.  Used mostly when reading file format headers, etc.
  /// This probably ought to be a derived number from the environment.
  const static int FILE_BLOCK_SIZE = 4096;

  HdfsScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);

  virtual ~HdfsScanner();

  /// One-time initialisation of state that is constant across scan ranges.
  virtual Status Open(ScannerContext* context) WARN_UNUSED_RESULT;

  /// Returns the next row batch from this scanner's split.
  /// Recoverable errors are logged to the runtime state. Only returns a non-OK status
  /// if a non-recoverable error is encountered (or abort_on_error is true). If OK is
  /// returned, 'parse_status_' is guaranteed to be OK as well.
  /// The memory referenced by the tuples is valid until this or any subsequently
  /// returned batch is reset or destroyed.
  /// Only valid to call if the parent scan node is single-threaded.
  Status GetNext(RowBatch* row_batch) WARN_UNUSED_RESULT {
    DCHECK(!scan_node_->HasRowBatchQueue());
    return GetNextInternal(row_batch);
  }

  /// Process an entire split, reading bytes from the context's streams.  Context is
  /// initialized with the split data (e.g. template tuple, partition descriptor, etc).
  /// This function should only return on error or end of scan range.
  /// Only valid to call if the parent scan node is multi-threaded.
  virtual Status ProcessSplit() WARN_UNUSED_RESULT;

  /// Creates a new row batch and transfers the ownership of memory backing returned
  /// tuples to it by calling Close(RowBatch). That last batch is added to the row batch
  /// queue. Only valid to call if HasRowBatchQueue().
  void Close();

  /// Transfers the ownership of memory backing returned tuples such as IO buffers
  /// and memory in mem pools to the given row batch. If the row batch is NULL,
  /// those resources are released instead. In any case, releases all other resources
  /// that are not backing returned rows (e.g. temporary decompression buffers).
  /// This function is not idempotent and must only be called once.
  virtual void Close(RowBatch* row_batch) = 0;

  /// Helper function that frees resources common to all scanner subclasses like the
  /// 'decompressor_', 'context_', 'obj_pool_', etc. Should only be called once the last
  /// row batch has been attached to the row batch queue (if applicable) to avoid freeing
  /// memory that might be referenced by the last batch.
  /// Only valid to call if 'is_closed_' is false. Sets 'is_closed_' to true.
  void CloseInternal();

  /// Only valid to call if the parent scan node is single-threaded.
  bool eos() const {
    DCHECK(!scan_node_->HasRowBatchQueue());
    return eos_;
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
  /// void IssueInitialRanges(HdfsScanNodeBase* scan_node,
  ///                         const std::vector<HdfsFileDesc*>& files);

  /// Codegen all functions for this scanner.  The codegen'd function is specific to
  /// the scanner subclass but not specific to each scanner object.  We don't want to
  /// codegen the functions for each scanner object.
  /// llvm::Function* Codegen(HdfsScanNode* scan_node);

  static const char* LLVM_CLASS_NAME;

 protected:
  /// The scan node that started this scanner
  HdfsScanNodeBase* scan_node_;

  /// RuntimeState for error reporting
  RuntimeState* state_;

  /// Context for this scanner
  ScannerContext* context_ = nullptr;

  /// Object pool for objects with same lifetime as scanner.
  ObjectPool obj_pool_;

  /// The first stream for context_
  ScannerContext::Stream* stream_ = nullptr;

  /// Set if this scanner has processed all ranges and will not produce more rows.
  bool eos_ = false;

  /// Starts as false and is set to true in Close().
  bool is_closed_ = false;

  /// MemPool used for expr-managed memory in expression evaluators in this scanner.
  /// Need to be local to each scanner as MemPool is not thread safe.
  boost::scoped_ptr<MemPool> expr_perm_pool_;

  /// Clones of the conjuncts' evaluators in scan_node_->conjuncts_map().
  /// Each scanner has its own ScalarExprEvaluators so the conjuncts can be safely
  /// evaluated in parallel.
  HdfsScanNodeBase::ConjunctEvaluatorsMap conjunct_evals_map_;

  // Convenience reference to conjuncts_evals_map_[scan_node_->tuple_idx()] for
  // scanners that do not support nested types.
  const std::vector<ScalarExprEvaluator*>* conjunct_evals_ = nullptr;

  // Clones of the conjuncts' evaluators in scan_node_->dict_filter_conjuncts_map().
  typedef std::map<SlotId, std::vector<ScalarExprEvaluator*>> DictFilterConjunctsMap;
  DictFilterConjunctsMap dict_filter_map_;

  /// Holds memory for template tuples. The memory in this pool must remain valid as long
  /// as the row batches produced by this scanner. This typically means that the
  /// ownership is transferred to the last row batch in Close(). Some scanners transfer
  /// the ownership to the parent scan node instead due being closed multiple times.
  boost::scoped_ptr<MemPool> template_tuple_pool_;

  /// A template tuple is a partially-materialized tuple with only partition key slots set
  /// (or other default values, such as NULL for columns missing in a file).  The other
  /// slots are set to NULL.  The template tuple must be copied into output tuples before
  /// any of the other slots are materialized.
  ///
  /// Each tuple descriptor (i.e. scan_node_->tuple_desc() and any collection item tuple
  /// descs) has a template tuple, or NULL if there are no partition key or default slots.
  /// Template tuples are computed once for each file and are allocated from
  /// template_tuple_pool_.
  std::unordered_map<const TupleDescriptor*, Tuple*> template_tuple_map_;

  /// Convenience variable set to the top-level template tuple
  /// (i.e. template_tuple_map_[scan_node_->tuple_desc()]).
  Tuple* template_tuple_ = nullptr;

  /// Fixed size of each top-level tuple, in bytes
  const int32_t tuple_byte_size_;

  /// Current tuple pointer into 'tuple_mem_'.
  Tuple* tuple_ = nullptr;

  /// The tuple memory backing 'tuple_'.
  uint8_t* tuple_mem_ = nullptr;

  /// Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  /// Contains current parse status to minimize the number of Status objects returned.
  /// This significantly minimizes the cross compile dependencies for llvm since status
  /// objects inline a bunch of string functions.  Also, status objects aren't extremely
  /// cheap to create and destroy.
  Status parse_status_ = Status::OK();

  /// Decompressor class to use, if any.
  boost::scoped_ptr<Codec> decompressor_;

  /// The most recently used decompression type.
  THdfsCompression::type decompression_type_ = THdfsCompression::NONE;

  /// Pool to allocate per data block memory.  This should be used with the
  /// decompressor and any other per data block allocations.
  boost::scoped_ptr<MemPool> data_buffer_pool_;

  /// Time spent decompressing bytes.
  RuntimeProfile::Counter* decompress_timer_ = nullptr;

  /// Matching typedef for WriteAlignedTuples for codegen.  Refer to comments for
  /// that function.
  typedef int (*WriteTuplesFn)(HdfsScanner*, MemPool*, TupleRow*, int, FieldLocation*,
      int, int, int, int);
  /// Jitted write tuples function pointer.  Null if codegen is disabled.
  WriteTuplesFn write_tuples_fn_ = nullptr;

  /// Implements GetNext(). Should be overridden by subclasses.
  /// Only valid to call if the parent scan node is multi-threaded.
  virtual Status GetNextInternal(RowBatch* row_batch) WARN_UNUSED_RESULT {
    DCHECK(false) << "GetNextInternal() not implemented for this scanner type.";
    return Status::OK();
  }

  /// Initializes write_tuples_fn_ to the jitted function if codegen is possible.
  /// - partition - partition descriptor for this scanner/scan range
  /// - type - type for this scanner
  /// - scanner_name - debug string name for this scanner (e.g. HdfsTextScanner)
  Status InitializeWriteTuplesFn(HdfsPartitionDescriptor* partition,
      THdfsFileFormat::type type, const std::string& scanner_name) WARN_UNUSED_RESULT;

  /// Reset internal state for a new scan range.
  virtual Status InitNewRange() WARN_UNUSED_RESULT = 0;

  /// Gets memory for outputting tuples into the CollectionValue being constructed via
  /// 'builder'. If memory limit is exceeded, an error status is returned. Otherwise,
  /// returns the maximum number of tuples that can be output in 'num_rows'.
  ///
  /// The returned TupleRow* should not be incremented (i.e. don't call next_row() on
  /// it). Instead, incrementing *tuple_mem will update *tuple_row_mem to be pointing at
  /// the next tuple. This also means its unnecessary to call
  /// (*tuple_row_mem)->SetTuple().
  Status GetCollectionMemory(CollectionValueBuilder* builder, MemPool** pool,
      Tuple** tuple_mem, TupleRow** tuple_row_mem, int64_t* num_rows) WARN_UNUSED_RESULT;

  /// Commits 'num_rows' to 'row_batch'. Advances 'tuple_mem_' and 'tuple_' accordingly.
  /// Attaches completed resources from 'context_' to 'row_batch' if necessary.
  /// Frees expr result allocations. Returns non-OK if 'context_' is cancelled or the
  /// query status in 'state_' is non-OK.
  Status CommitRows(int num_rows, RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Convenience function for evaluating conjuncts using this scanner's ScalarExprEvaluators.
  /// This must always be inlined so we can correctly replace the call to
  /// ExecNode::EvalConjuncts() during codegen.
  bool IR_ALWAYS_INLINE EvalConjuncts(TupleRow* row)  {
    return ExecNode::EvalConjuncts(conjunct_evals_->data(), conjunct_evals_->size(), row);
  }

  /// Sets 'num_tuples' template tuples in the batch that 'row' points to. Assumes the
  /// 'tuple_row' only has a single tuple. Returns the number of tuples set.
  int WriteTemplateTuples(TupleRow* row, int num_tuples);

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
  /// Only valid to call if the parent scan node is multi-threaded.
  int WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row_mem, int row_size,
      FieldLocation* fields, int num_tuples,
      int max_added_tuples, int slots_per_tuple, int row_start_indx);

  /// Update the decompressor_ object given a compression type or codec name. Depending on
  /// the old compression type and the new one, it may close the old decompressor and/or
  /// create a new one of different type.
  Status UpdateDecompressor(const THdfsCompression::type& compression) WARN_UNUSED_RESULT;
  Status UpdateDecompressor(const std::string& codec) WARN_UNUSED_RESULT;

  /// Utility function to report parse errors for each field.
  /// If errors[i] is nonzero, fields[i] had a parse error.
  /// Returns false if parsing should be aborted.  In this case parse_status_ is set
  /// to the error.
  /// This is called from WriteAlignedTuples.
  bool ReportTupleParseError(FieldLocation* fields, uint8_t* errors);

  /// Triggers debug action of the scan node. This is currently used by Parquet column
  /// readers to exercise various failure paths in Parquet scanner. Returns the status
  /// returned by the scan node's TriggerDebugAction().
  Status ScannerDebugAction() WARN_UNUSED_RESULT {
    return scan_node_->ScanNodeDebugAction(TExecNodePhase::GETNEXT_SCANNER);
  }

  /// Utility function to append an error message for an invalid row.
  void LogRowParseError();

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
      uint8_t* error_in_row) WARN_UNUSED_RESULT;

  /// Codegen function to replace WriteCompleteTuple. Should behave identically
  /// to WriteCompleteTuple. Stores the resulting function in 'write_complete_tuple_fn'
  /// if codegen was successful or NULL otherwise.
  static Status CodegenWriteCompleteTuple(HdfsScanNodeBase* node, LlvmCodeGen* codegen,
      const std::vector<ScalarExpr*>& conjuncts,
      llvm::Function** write_complete_tuple_fn)
      WARN_UNUSED_RESULT;

  /// Codegen function to replace WriteAlignedTuples.  WriteAlignedTuples is cross
  /// compiled to IR.  This function loads the precompiled IR function, modifies it,
  /// and stores the resulting function in 'write_aligned_tuples_fn' if codegen was
  /// successful or NULL otherwise.
  static Status CodegenWriteAlignedTuples(HdfsScanNodeBase*, LlvmCodeGen*,
      llvm::Function* write_tuple_fn, llvm::Function** write_aligned_tuples_fn)
      WARN_UNUSED_RESULT;

  /// Report parse error for column @ desc.   If abort_on_error is true, sets
  /// parse_status_ to the error message.
  void ReportColumnParseError(const SlotDescriptor* desc, const char* data, int len);

  /// Initialize a tuple.
  /// TODO: only copy over non-null slots.
  void InitTuple(const TupleDescriptor* desc, Tuple* template_tuple, Tuple* tuple) {
    if (template_tuple != NULL) {
      InitTupleFromTemplate(template_tuple, tuple, desc->byte_size());
    } else {
      tuple->ClearNullBits(*desc);
    }
  }

  /// Initialize 'tuple' with size 'tuple_byte_size' from 'template_tuple'
  void InitTupleFromTemplate(Tuple* template_tuple, Tuple* tuple, int tuple_byte_size) {
    memcpy(tuple, template_tuple, tuple_byte_size);
  }

  /// Initialize a dense array of 'num_tuples' tuples.
  /// TODO: we could do better here if we inlined the tuple and null indicator byte
  /// widths with codegen to eliminate all the branches in memcpy()/memset().
  void InitTupleBuffer(
      Tuple* template_tuple, uint8_t* __restrict__ tuple_mem, int64_t num_tuples) {
    const TupleDescriptor* desc = scan_node_->tuple_desc();
    const int tuple_byte_size = desc->byte_size();
    // Handle the different template/non-template cases with different loops to avoid
    // unnecessary branches inside the loop.
    if (template_tuple != nullptr) {
      for (int64_t i = 0; i < num_tuples; ++i) {
        InitTupleFromTemplate(
            template_tuple, reinterpret_cast<Tuple*>(tuple_mem), tuple_byte_size);
        tuple_mem += tuple_byte_size;
      }
    } else if (tuple_byte_size <= CACHE_LINE_SIZE) {
      // If each tuple fits in a cache line, it is quicker to zero the whole memory buffer
      // instead of just the null indicators. This is because we are fetching the cache
      // line anyway and zeroing a cache line is cheap (a couple of AVX2 instructions)
      // compared with the overhead of calling memset() row-by-row.
      memset(tuple_mem, 0, num_tuples * tuple_byte_size);
    } else {
      const int null_bytes_offset = desc->null_bytes_offset();
      const int num_null_bytes = desc->num_null_bytes();
      for (int64_t i = 0; i < num_tuples; ++i) {
        reinterpret_cast<Tuple*>(tuple_mem)->ClearNullBits(
            null_bytes_offset, num_null_bytes);
        tuple_mem += tuple_byte_size;
      }
    }
  }

  void InitTuple(Tuple* template_tuple, Tuple* tuple) {
    InitTuple(scan_node_->tuple_desc(), template_tuple, tuple);
  }

  inline Tuple* next_tuple(int tuple_byte_size, Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + tuple_byte_size);
  }

  /// Assumes the row only has a single tuple.
  inline TupleRow* next_row(TupleRow* r) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(r);
    return reinterpret_cast<TupleRow*>(mem + sizeof(Tuple*));
  }

  /// Simple wrapper around conjunct_evals_[idx]. Used in the codegen'd version of
  /// WriteCompleteTuple() because it's easier than writing IR to access
  /// conjunct_evals_.
  ScalarExprEvaluator* GetConjunctEval(int idx) const;

  /// Unit test constructor
  HdfsScanner();
};

}

#endif

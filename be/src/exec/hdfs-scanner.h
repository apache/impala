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


#ifndef IMPALA_EXEC_HDFS_SCANNER_H_
#define IMPALA_EXEC_HDFS_SCANNER_H_

#include <vector>
#include <memory>
#include <stdint.h>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>

#include "exec/scan-node.h"
#include "runtime/disk-io-mgr.h"

namespace impala {

class ByteStream;
class Compression;
class DescriptorTbl;
class Expr;
class HdfsPartitionDescriptor;
class HdfsScanNode;
class MemPool;
class RowBatch;
class ScanRangeContext;
class SlotDescriptor;
class Status;
class TextConverter;
class Tuple;
class TupleDescriptor;
class TPlanNode;
class TScanRange;

// Intermediate structure used for two pass parsing approach. In the first pass,
// the FieldLocation structs are filled out and contain where all the fields start and
// their lengths.  In the second pass, the FieldLocation is used to write out the
// slots. We want to keep this struct as small as possible.
struct FieldLocation {
  //start of field
  char* start;
  // Encodes the length and whether or not this fields needs to be unescaped.
  // If len < 0, then the field needs to be unescaped.
  int len;

  static const char* LLVM_CLASS_NAME;
};

// TODO: Remove this comment when all scanners are updated to use IO mgr.  See
// ScanRangeContext comments for how the io mgr based scanners work.
// HdfsScanners are instantiated by an HdfsScanNode to parse file data in a particular
// format into Impala's Tuple structures. They are an abstract class; the actual mechanism
// for parsing bytes into tuples is format specific and supplied by subclasses.  This
// class provides methods to allocate tuple buffer data to write tuples to, and access to
// the parent scan node to retrieve information that is constant across scan range
// boundaries. The scan node provides a ByteStream* for reading bytes from (usually) an
// HdfsByteStream. Management of any buffer space to stage those bytes is the
// responsibility of the subclass.  The scan node also provides a template tuple
// containing pre-materialised slots to initialise each tuple.
// Subclasses must implement GetNext. They must also call 
// scan_node_->IncrNumRowsReturned() for every row they write.
// The typical lifecycle is:
//   1. Prepare
//   2. InitCurrentScanRange
//   3. GetNext
//   4. Repeat 3 until scan range is exhausted
//   5. Repeat from 2., if there are more scan ranges in the current file.
// For codegen, the functionality is split into two parts.  
//   1. During the Prepare() phase, the scanner subclass's Codegen() function will be
//      called to perform codegen for that scanner type for the specific tuple desc.
//      This codegen'd function is cached in the HdfsScanNode.
//   2. During the GetNext() phase (where we create one Scanner for each scan range),
//      the created scanner subclass can retreive, from the scan node, the codegen'd
//      function to use.  
// This way, we only codegen once per scanner type, rather than once per scanner object.
class HdfsScanner {
 public:
  // Assumed size of a OS file block.  Used mostly when reading file format headers, etc.
  // This probably ought to be a derived number from the environment.
  const static int FILE_BLOCK_SIZE = 4096;

  // scan_node - parent scan node
  // filling materialized slots
  // tuple_pool - mem pool owned by parent scan node for allocation
  // of tuple buffer data
  HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state, MemPool* tuple_pool);

  virtual ~HdfsScanner();

  // Writes parsed tuples into tuple buffer,
  // and sets pointers in row_batch to point to them.
  // row_batch may be non-full when all scan ranges have been read
  virtual Status GetNext(RowBatch* row_batch, bool* eos) { 
    // TODO: remove when all scanners are updated
    DCHECK(false);
    return Status::OK;
  }

  // One-time initialisation of state that is constant across scan ranges.
  // TODO: update comment when all scanners are using the io mgr.
  virtual Status Prepare();

  // Scanner subclasses must implement these static functions as well.  Unfortunately,
  // c++ does not allow static virtual functions.

  // Issue the initial ranges for 'files'
  // void IssueInitialRanges(HdfsScanNode*, const std::vector<HdfsFileDesc*>& files);

  // Codegen all functions for this scanner.  The codegen'd function is specific to
  // the scanner subclass but not specific to each scanner object.  We don't want to
  // codegen the functions for each scanner object.
  // llvm::Function* Codegen(HdfsScanNode*);

  // One-time initialisation of per-scan-range state.  The scanner objects are
  // reused for different scan ranges so this function must reset all state.
  virtual Status InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
      DiskIoMgr::ScanRange* scan_range, Tuple* template_tuple, ByteStream* byte_stream);

  // Process an entire scan range reading bytes from context.  Context is initialized
  // with the scan range meta data (e.g. template tuple, partition descriptor, etc).
  // This function should only return on error or end of scan range.
  virtual Status ProcessScanRange(ScanRangeContext* context) {
    // TODO: make this pure virtual when all scanners are updated.
    return Status::OK;
  }

  // Release all resources the scanner has allocated.  This is the last chance for
  // the scanner to attach any resources to the ScanRangeContext object.
  virtual Status Close() = 0;

  static const char* LLVM_CLASS_NAME;
  
 protected:
  // For EvalConjunctsForScanner
  HdfsScanNode* scan_node_;

  // RuntimeState for error reporting
  RuntimeState* state_;
  
  // Context for this scan range
  ScanRangeContext* context_;

  // Conjuncts for this scanner.  Multiple scanners from multiple threads can
  // be scanning and Exprs are currently not thread safe.
  std::vector<Expr*> conjuncts_mem_;

  // Cache of &conjuncts[0] to avoid using vector functions.
  Expr** conjuncts_;

  // Cache of conjuncts_mem_.size()
  int num_conjuncts_;

  // Contiguous block of memory into which tuples are written, allocated
  // from tuple_pool_. We don't allocate individual tuples from tuple_pool_ directly,
  // because MemPool::Allocate() always rounds up to the next 8 bytes
  // (ie, would be wasteful if we had 2-byte tuples).
  uint8_t* tuple_buffer_;

  // Size of tuple_buffer_.
  int tuple_buffer_size_;

  // Fixed size of each tuple, in bytes
  int tuple_byte_size_;

  // Contains all memory for tuple data, including string data which
  // we can't reference in the file buffers (because it needs to be
  // unescaped or straddles two file buffers). Owned by the parent
  // scan node, since the data in it may need to outlive this scanner.
  MemPool* tuple_pool_;

  // Current tuple.
  Tuple* tuple_;

  // number of errors in current file
  int num_errors_in_file_;

  // The current provider of bytes to parse. Usually only changes
  // between files, so is active for the lifetime of a scanner.
  ByteStream* current_byte_stream_;
 
  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // A partially materialized tuple with only partition key slots set.
  // The non-partition key slots are set to NULL.  The template tuple
  // must be copied into tuple_ before any of the other slots are
  // materialized.
  // Pointer is NULL if there are no partition key slots.
  // This template tuple is computed once for each file and valid for
  // the duration of that file.
  Tuple* template_tuple_;

  // True if the descriptor of the tuple the scanner is writing has
  // string slots and we are not compacting data. This is used to decide
  // how to treat buffer memory that contains slot data.
  bool has_noncompact_strings_;
  
  // The scan range currently being read
  DiskIoMgr::ScanRange* current_scan_range_;
  
  // Number of null bytes in the tuple.
  int32_t num_null_bytes_;

  // Contains current parse status to minimize the number of Status objects returned.
  // This significantly minimizes the cross compile dependencies for llvm since status
  // objects inline a bunch of string functions.  Also, status objects aren't extremely
  // cheap to create and destroy.
  Status parse_status_;

  // Matching typedef for WriteAlignedTuples for codegen.  Refer to comments for
  // that function.
  typedef int (*WriteTuplesFn)(HdfsScanner*, MemPool*, TupleRow*, int, FieldLocation*, 
      int, int, int, int);
  // Jitted write tuples function pointer.  Null if codegen is disabled.
  WriteTuplesFn write_tuples_fn_;

  // Create a copy of the conjuncts. Exprs are not thread safe (they store results 
  // inside the expr) so each scanner needs its own copy.  This is not needed for
  // codegen'd scanners since they evaluate exprs with a different mechanism.
  // TODO: fix exprs
  Status CreateConjunctsCopy();

  // Initializes write_tuples_fn_ to the jitted function if codegen is possible.
  // - partition - partition descriptor for this scanner/scan range
  // - type - type for this scanner
  // - scanner_name - debug string name for this scanner (e.g. HdfsTextScanner)
  Status InitializeCodegenFn(HdfsPartitionDescriptor* partition, 
      THdfsFileFormat::type type, const std::string& scanner_name);

  // Allocates a buffer from tuple_pool_ large enough to hold one
  // tuple for every remaining row in row_batch.  The tuple memory
  // is uninitialized.
  void AllocateTupleBuffer(RowBatch* row_batch);

  // Utility method to write out tuples when there are no materialized
  // fields (e.g. select count(*) or only partition keys).
  //   num_tuples - Total number of tuples to write out.
  // Returns the number of tuples added to the row batch.
  int WriteEmptyTuples(RowBatch* row_batch, int num_tuples);

  // Write empty tuples and commit them to the context object
  int WriteEmptyTuples(ScanRangeContext* context, TupleRow* tuple_row, int num_tuples);

  // Processes batches of fields and writes them out to tuple_row_mem.
  // - 'pool' mempool to allocate from for auxiliary tuple memory
  // - 'tuple_row_mem' preallocated tuple_row memory this function must use.
  // - 'fields' must start at the beginning of a tuple.
  // - 'num_tuples' number of tuples to process
  // - 'max_added_tuples' the maximum number of tuples that should be added to the batch.
  // - 'row_start_index' is the number of rows that have already been processed
  //   as part of WritePartialTuple.  
  // Returns the number of tuples added to the row batch.  This can be less than
  // num_tuples/tuples_till_limit because of failed conjuncts.
  // Returns -1 if parsing should be aborted due to parse errors.
  int WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row_mem, int row_size, 
      FieldLocation* fields, int num_tuples,
      int max_added_tuples, int slots_per_tuple, int row_start_indx);

  // Utility function to report parse errors for each field.
  // If errors[i] is nonzero, fields[i] had a parse error.
  // row_idx is the idx of the row in the current batch that had the parse error
  // Returns false if parsing should be aborted.  In this case parse_status_ is set
  // to the error.
  // This is called from WriteAlignedTuples.  
  bool ReportTupleParseError(FieldLocation* fields, uint8_t* errors, int row_idx);

  // Utility function to append an error message for an invalid row.  This is called
  // from ReportTupleParseError()
  // row_idx is the index of the row in the current batch.  Subclasses should override
  // this function (i.e. text needs to join boundary rows).  Since this is only in the
  // error path, vtable overhead is acceptable.
  virtual void LogRowParseError(std::stringstream*, int row_idx);

  // Writes out all slots for 'tuple' from 'fields'. 'fields' must be aligned
  // to the start of the tuple (e.g. fields[0] maps to slots[0]).
  // After writing the tuple, it will be evaluated against the conjuncts.
  //  - error_fields is an out array.  error_fields[i] will be set to true if the ith
  //    field had a parse error
  //  - error_in_row is an out bool.  It is set to true if any field had parse errors
  // Returns whether the resulting tuplerow passed the conjuncts.
  //
  // The parsing of the fields and evaluating against conjuncts is combined in this
  // function.  This is done so it can be possible to evaluate conjuncts as slots
  // are materialized (on partial tuples).  
  //
  // This function is replaced by a codegen'd function at runtime.  This is
  // the reason that the out error parameters are typed uint8_t instead of bool. We need
  // to be able to match this function's signature identically for the codegen'd function.
  // Bool's as out parameters can get converted to bytes by the compiler and rather than
  // implicitly depending on that to happen, we will explicitly type them to bytes.
  // TODO: revisit this
  bool WriteCompleteTuple(MemPool* pool, FieldLocation* fields, Tuple* tuple, 
      TupleRow* tuple_row, Tuple* template_tuple, uint8_t* error_fields, 
      uint8_t* error_in_row);

  // Codegen function to replace WriteCompleteTuple. Should behave identically
  // to WriteCompleteTuple.
  static llvm::Function* CodegenWriteCompleteTuple(HdfsScanNode*, LlvmCodeGen*);
  
  // Codegen function to replace WriteAlignedTuples.  WriteAlignedTuples is cross compiled
  // to IR.  This function loads the precompiled IR function, modifies it and returns the
  // resulting function.
  static llvm::Function* CodegenWriteAlignedTuples(HdfsScanNode*, LlvmCodeGen*, 
      llvm::Function* write_tuple_fn);
  
  // Report parse error for column @ desc
  void ReportColumnParseError(const SlotDescriptor* desc, const char* data, int len);

  // Initialize a tuple.
  // TODO: only copy over non-null slots.
  void InitTuple(Tuple* template_tuple, Tuple* tuple) {
    if (template_tuple != NULL) {
      memcpy(tuple, template_tuple, tuple_byte_size_);
    } else {
      memset(tuple, 0, sizeof(uint8_t) * num_null_bytes_);
    }
  }
};

}

#endif

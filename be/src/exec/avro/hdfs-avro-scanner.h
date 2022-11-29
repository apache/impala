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


#ifndef IMPALA_EXEC_HDFS_AVRO_SCANNER_H
#define IMPALA_EXEC_HDFS_AVRO_SCANNER_H

/// This scanner reads Avro object container files (i.e., Avro data files) and writes the
/// content as tuples in the Impala in-memory representation of data (tuples, rows,
/// row batches).
///
/// The specification for Avro files can be found at
/// http://avro.apache.org/docs/current/spec.html (the current Avro version is
/// 1.7.4 as of the time of this writing). At a high level, an Avro data file has
/// the following structure:
///
/// - Avro data file
///   - file header
///     - file version header
///     - file metadata
///       - JSON schema
///       - compression codec (optional)
///     - sync marker
///   - data block+
///     - # of Avro objects in block
///     - size of objects in block (post-compression)
///     - serialized objects
///     - sync marker
///
///
/// This implementation reads one data block at a time, using the schema from the file
/// header to decode the serialized objects. If possible, non-materialized columns are
/// skipped without being read. If codegen is enabled, we codegen a function based on the
/// table schema that parses records, materializes them to tuples, and evaluates the
/// conjuncts.
///
/// The Avro C library is used to parse the file's schema and the table's schema, which are
/// then resolved according to the Avro spec and transformed into our own schema
/// representation (i.e. a list of SchemaElements). Schema resolution allows users to
/// evolve the table schema and file schema(s) independently. The spec goes over all the
/// rules for schema resolution, but in summary:
///
/// - Record fields are matched by name (and thus can be reordered; the table schema
///   determines the order of the columns)
/// - Fields in the file schema not present in the table schema are ignored
/// - Fields in the table schema not present in the file schema must have a default value
///   specified
/// - Types can be "promoted" as follows:
///   int -> long -> float -> double
///
/// TODO:
/// - implement SkipComplex()
/// - codegen a function per unique file schema, rather than just the table schema
/// - once Exprs are thread-safe, we can cache the jitted function directly
/// - microbenchmark codegen'd functions (this and other scanners)

#include "exec/base-sequence-scanner.h"

#include <avro/basics.h>

#include "exec/read-write-util.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace llvm {
  class BasicBlock;
  class Value;
}

namespace impala {

class HdfsAvroScanner : public BaseSequenceScanner {
 public:
  /// The four byte Avro version header present at the beginning of every
  /// Avro file: {'O', 'b', 'j', 1}
  static const uint8_t AVRO_VERSION_HEADER[4];

  HdfsAvroScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);

  virtual Status Open(ScannerContext* context) WARN_UNUSED_RESULT;

  /// Codegen DecodeAvroData(). Stores the resulting function in 'decode_avro_data_fn' if
  /// codegen was successful or nullptr otherwise.
  static Status Codegen(
      HdfsScanPlanNode* node, FragmentState* state, llvm::Function** decode_avro_data_fn);

  static const char* LLVM_CLASS_NAME;

 protected:
  /// Implementation of BaseSeqeunceScanner super class methods
  virtual FileHeader* AllocateFileHeader();
  /// TODO: check that file schema matches metadata schema
  virtual Status ReadFileHeader() WARN_UNUSED_RESULT;
  virtual Status InitNewRange() WARN_UNUSED_RESULT;
  virtual Status ProcessRange(RowBatch* row_batch) WARN_UNUSED_RESULT;

  virtual THdfsFileFormat::type file_format() const { return THdfsFileFormat::AVRO; }

 private:
  friend class HdfsAvroScannerTest;

  struct AvroFileHeader : public BaseSequenceScanner::FileHeader {
    /// The root of the file schema tree (i.e. the top-level record schema of the file)
    ScopedAvroSchemaElement schema;

    /// Template tuple for this file containing partition key values and default values.
    /// Set to nullptr if there are no materialized partition keys and no default values
    /// are necessary (i.e., all materialized fields are present in the file schema).
    /// This tuple is created by the scanner processing the initial scan range with
    /// the header. The ownership of memory is transferred to the template pool of
    /// ScanRangeSharedState, such that it remains live when subsequent scanners process
    /// data ranges.
    Tuple* template_tuple;

    /// True if this file can use the codegen'd version of DecodeAvroData() (i.e. its
    /// schema matches the table schema), false otherwise.
    bool use_codegend_decode_avro_data;
  };

  AvroFileHeader* avro_header_ = nullptr;

  /// Current data block after decompression with its end and length.
  uint8_t* data_block_ = nullptr;
  uint8_t* data_block_end_ = nullptr;
  int64_t data_block_len_ = 0;

  /// Number of records in the current data block and the current record position.
  int64_t num_records_in_block_ = 0;
  int64_t record_pos_ = 0;

  /// Metadata keys
  static const std::string AVRO_SCHEMA_KEY;
  static const std::string AVRO_CODEC_KEY;

  /// Supported codecs, as they appear in the metadata
  static const std::string AVRO_NULL_CODEC;
  static const std::string AVRO_SNAPPY_CODEC;
  static const std::string AVRO_DEFLATE_CODEC;

  typedef int (*DecodeAvroDataFn)(HdfsAvroScanner*, int, MemPool*, uint8_t**, uint8_t*,
                                  Tuple*, TupleRow*);

  /// The codegen'd version of DecodeAvroData() if available, nullptr otherwise.
  /// Function type: DecodeAvroDataFn.
  const CodegenFnPtrBase* codegend_decode_avro_data_ = nullptr;

  /// Utility function for decoding and parsing file header metadata
  Status ParseMetadata() WARN_UNUSED_RESULT;

  /// Resolves the table schema (i.e. the reader schema) against the file schema (i.e. the
  /// writer schema), and sets the 'slot_desc' fields of the nodes of the file schema
  /// corresponding to materialized slots. Calls WriteDefaultValue() as
  /// appropriate. Returns a non-OK status if the schemas could not be resolved.
  Status ResolveSchemas(const AvroSchemaElement& table_root,
      AvroSchemaElement* file_root) WARN_UNUSED_RESULT;

  // Returns Status::OK iff table_schema (the reader schema) can be resolved against
  // file_schema (the writer schema). field_name is used for error messages.
  Status VerifyTypesMatch(const AvroSchemaElement& table_schema,
      const AvroSchemaElement& file_schema, const string& field_name) WARN_UNUSED_RESULT;

  /// Returns Status::OK iff a value with the given schema can be used to populate
  /// 'slot_desc', as if 'schema' were the writer schema and 'slot_desc' the reader
  /// schema. 'schema' can be either a avro_schema_t or avro_datum_t.
  Status VerifyTypesMatch(SlotDescriptor* slot_desc, avro_obj_t* schema)
      WARN_UNUSED_RESULT;

  /// Return true if reader_type can be used to read writer_type according to the Avro
  /// type promotion rules. Note that this does not handle nullability or TYPE_NULL.
  bool VerifyTypesMatch(const ColumnType& reader_type, const ColumnType& writer_type);

  /// Writes 'default_value' to 'slot_desc' in the template tuple, initializing the
  /// template tuple if it doesn't already exist. Returns a non-OK status if slot_desc's
  /// and default_value's types are incompatible or unsupported. field_name is used for
  /// error messages.
  Status WriteDefaultValue(SlotDescriptor* slot_desc, avro_datum_t default_value,
      const char* field_name) WARN_UNUSED_RESULT;

  /// Decodes records and copies the data into tuples.
  /// Returns the number of tuples to be committed.
  /// - max_tuples: the maximum number of tuples to write
  /// - data: serialized record data. Is advanced as records are read.
  /// - data_end: pointer to the end of the data buffer (i.e. the first invalid byte).
  /// - pool: memory pool to allocate string data from
  /// - tuple: tuple pointer to copy objects to
  /// - tuple_row: tuple row of written tuples
  int DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data, uint8_t* data_end,
      Tuple* tuple, TupleRow* tuple_row);

  int DecodeAvroDataCodegenOrInterpret(int max_tuples, MemPool* pool, uint8_t** data,
      uint8_t* data_end, Tuple* tuple, TupleRow* tuple_row);


  /// Materializes a single tuple from serialized record data. Will return false and set
  /// error in parse_status_ if memory limit is exceeded when allocating new char buffer.
  /// See comments below for ReadAvroChar().
  bool MaterializeTuple(const AvroSchemaElement& record_schema, MemPool* pool,
      uint8_t** data, uint8_t* data_end, Tuple* tuple);

  /// Produces a version of DecodeAvroData that uses codegen'd instead of interpreted
  /// functions. Stores the resulting function in 'decode_avro_data_fn' if codegen was
  /// successful or returns an error.
  static Status CodegenDecodeAvroData(const HdfsScanPlanNode* node, FragmentState* state,
      llvm::Function** decode_avro_data_fn);

  /// Codegens a version of MaterializeTuple() that reads records based on the table
  /// schema. Stores the resulting function in 'materialize_tuple_fn' if codegen was
  /// successful or returns an error.
  /// TODO: Codegen a function for each unique file schema.
  static Status CodegenMaterializeTuple(const HdfsScanPlanNode* node,
      LlvmCodeGen* codegen, llvm::Function** materialize_tuple_fn) WARN_UNUSED_RESULT;

  /// Used by CodegenMaterializeTuple to recursively create the IR for reading an Avro
  /// record.
  /// - path: the column path constructed so far. This is used to find the slot desc, if
  ///     any, associated with each field of the record. Note that this assumes the
  ///     table's Avro schema matches up with the table's column definitions by ordinal.
  /// - builder: used to insert the IR, starting at the current insert point. The insert
  ///     point will be left at the end of the record but before the 'insert_before'
  ///     block.
  /// - insert_before: the block to insert any new blocks directly before. This is either
  ///     the bail_out block or some basic blocks before that.
  /// - bail_out: the block to jump to if anything fails. This is used in particular by
  ///     ReadAvroChar() which can exceed memory limit during allocation from MemPool.
  /// - this_val, pool_val, tuple_val, data_val, data_end_val: arguments to
  ///     MaterializeTuple()
  /// - child_start / child_end: specifies to only generate a subset of the record
  ///     schema's children
  static Status CodegenReadRecord(const SchemaPath& path, const AvroSchemaElement& record,
      int child_start, int child_end, const HdfsScanPlanNode* node, LlvmCodeGen* codegen,
      void* builder, llvm::Function* fn, llvm::BasicBlock* insert_before,
      llvm::BasicBlock* bail_out, llvm::Value* this_val, llvm::Value* pool_val,
      llvm::Value* tuple_val, llvm::Value* data_val,
      llvm::Value* data_end_val) WARN_UNUSED_RESULT;

  /// Creates the IR for reading an Avro scalar at builder's current insert point.
  static Status CodegenReadScalar(const AvroSchemaElement& element,
      SlotDescriptor* slot_desc, LlvmCodeGen* codegen, void* void_builder,
      llvm::Value* this_val, llvm::Value* pool_val, llvm::Value* tuple_val,
      llvm::Value* data_val, llvm::Value* data_end_val, llvm::Value** ret_val)
      WARN_UNUSED_RESULT;

  /// The following are cross-compiled functions for parsing a serialized Avro primitive
  /// type and writing it to a slot. They can also be used for skipping a field without
  /// writing it to a slot by setting 'write_slot' to false.
  /// - data: Serialized record data. Is advanced past the read field.
  /// - data_end: pointer to the end of the data buffer (i.e. the first invalid byte).
  /// The following arguments are used only if 'write_slot' is true:
  /// - slot: The tuple slot to write the parsed field into.
  /// - type: The type of the slot. (This is necessary because there is not a 1:1 mapping
  ///         between Avro types and Impala's primitive types.)
  /// - pool: MemPool for string data.
  ///
  /// All return false and set parse_status_ on error (e.g. mem limit exceeded when
  /// allocating buffer, malformed data), and return true otherwise.
  ///
  bool ReadAvroBoolean(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroDate(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroInt32(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroInt64(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroFloat(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroDouble(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroVarchar(PrimitiveType type, int max_len, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroChar(PrimitiveType type, int max_len, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);
  bool ReadAvroString(PrimitiveType type, uint8_t** data, uint8_t* data_end,
      bool write_slot, void* slot, MemPool* pool);

  /// Helper function for some of the above. Returns the the length of certain varlen
  /// types and updates 'data'. If an error is encountered returns a non-ok result and
  /// updates parse_status_.
  ReadWriteUtil::ZLongResult ReadFieldLen(uint8_t** data, uint8_t* data_end);

  /// Same as the above functions, except takes the size of the decimal slot (i.e. 4, 8, or
  /// 16) instead of the type (which should be TYPE_DECIMAL). The slot size is passed
  /// explicitly, rather than passing a ColumnType, so we can easily pass in a constant in
  /// the codegen'd MaterializeTuple() function. If 'write_slot' is false, 'slot_byte_size'
  /// is ignored.
  bool ReadAvroDecimal(
      int slot_byte_size, uint8_t** data, uint8_t* data_end, bool write_slot, void* slot,
      MemPool* pool);

  /// Reads and advances 'data' past the union branch index and sets 'is_null' according
  /// to if the corresponding element is null. 'null_union_position' must be 0 or
  /// 1. Returns false and sets parse_status_ if there's an error, otherwise returns true.
  bool ReadUnionType(int null_union_position, uint8_t** data, uint8_t* data_end,
      bool* is_null);

  /// Helper functions to set parse_status_ outside of xcompiled functions. This is to
  /// avoid including string construction, etc. in the IR, which boths bloats it and can
  /// contain exception handling code.
  void SetStatusCorruptData(TErrorCode::type error_code);
  void SetStatusInvalidValue(TErrorCode::type error_code, int64_t len);
  void SetStatusValueOverflow(TErrorCode::type error_code, int64_t len, int64_t limit);

  /// Unit test constructor
  HdfsAvroScanner();
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

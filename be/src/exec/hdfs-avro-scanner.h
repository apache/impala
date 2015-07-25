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


#ifndef IMPALA_EXEC_HDFS_AVRO_SCANNER_H
#define IMPALA_EXEC_HDFS_AVRO_SCANNER_H

/// This scanner reads Avro object container files (i.e., Avro data files)
/// located in HDFS and writes the content as tuples in the Impala in-memory
/// representation of data (e.g. tuples, rows, row batches).
//
/// The specification for Avro files can be found at
/// http://avro.apache.org/docs/current/spec.html (the current Avro version is
/// 1.7.4 as of the time of this writing). At a high level, an Avro data file has
/// the following structure:
//
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
//
//
/// This implementation reads one data block at a time, using the schema from the file
/// header to decode the serialized objects. If possible, non-materialized columns are
/// skipped without being read. If codegen is enabled, we codegen a function based on the
/// table schema that parses records, materializes them to tuples, and evaluates the
/// conjuncts.
//
/// The Avro C library is used to parse the file's schema and the table's schema, which are
/// then resolved according to the Avro spec and transformed into our own schema
/// representation (i.e. a list of SchemaElements). Schema resolution allows users to
/// evolve the table schema and file schema(s) independently. The spec goes over all the
/// rules for schema resolution, but in summary:
//
/// - Record fields are matched by name (and thus can be reordered; the table schema
///   determines the order of the columns)
/// - Fields in the file schema not present in the table schema are ignored
/// - Fields in the table schema not present in the file schema must have a default value
///   specified
/// - Types can be "promoted" as follows:
///   int -> long -> float -> double
//
/// TODO:
/// - implement SkipComplex()
/// - codegen a function per unique file schema, rather than just the table schema
/// - once Exprs are thread-safe, we can cache the jitted function directly
/// - microbenchmark codegen'd functions (this and other scanners)

#include "exec/base-sequence-scanner.h"

#include <avro/basics.h>
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

  HdfsAvroScanner(HdfsScanNode* scan_node, RuntimeState* state);

  /// Codegen parsing records, writing tuples and evaluating predicates.
  static llvm::Function* Codegen(HdfsScanNode*,
                                 const std::vector<ExprContext*>& conjunct_ctxs);

 protected:
  /// Implementation of BaseSeqeunceScanner super class methods
  virtual FileHeader* AllocateFileHeader();
  /// TODO: check that file schema matches metadata schema
  virtual Status ReadFileHeader();
  virtual Status InitNewRange();
  virtual Status ProcessRange();

  virtual THdfsFileFormat::type file_format() const {
    return THdfsFileFormat::AVRO;
  }

 private:
  struct AvroFileHeader : public BaseSequenceScanner::FileHeader {
    /// The root of the file schema tree (i.e. the top-level record schema of the file)
    ScopedAvroSchemaElement schema;

    /// Template tuple for this file containing partition key values and default values.
    /// NULL if there are no materialized partition keys and no default values are
    /// necessary (i.e., all materialized fields are present in the file schema).
    /// template_tuple_ is set to this value.
    Tuple* template_tuple;

    /// True if this file can use the codegen'd version of DecodeAvroData() (i.e. its
    /// schema matches the table schema), false otherwise.
    bool use_codegend_decode_avro_data;
  };

  AvroFileHeader* avro_header_;

  /// Metadata keys
  static const std::string AVRO_SCHEMA_KEY;
  static const std::string AVRO_CODEC_KEY;

  /// Supported codecs, as they appear in the metadata
  static const std::string AVRO_NULL_CODEC;
  static const std::string AVRO_SNAPPY_CODEC;
  static const std::string AVRO_DEFLATE_CODEC;

  typedef int (*DecodeAvroDataFn)(HdfsAvroScanner*, int, MemPool*, uint8_t**,
                                  Tuple*, TupleRow*);

  /// The codegen'd version of DecodeAvroData() if available, NULL otherwise.
  DecodeAvroDataFn codegend_decode_avro_data_;

  /// Utility function for decoding and parsing file header metadata
  Status ParseMetadata();

  /// Resolves the table schema (i.e. the reader schema) against the file schema (i.e. the
  /// writer schema), and sets the 'slot_desc' fields of the nodes of the file schema
  /// corresponding to materialized slots. Calls WriteDefaultValue() as
  /// appropriate. Returns a non-OK status if the schemas could not be resolved.
  Status ResolveSchemas(const AvroSchemaElement& table_root,
                        AvroSchemaElement* file_root);

  // Returns Status::OK iff table_schema (the reader schema) can be resolved against
  // file_schema (the writer schema). field_name is used for error messages.
  Status VerifyTypesMatch(const AvroSchemaElement& table_schema,
      const AvroSchemaElement& file_schema, const string& field_name);

  /// Returns Status::OK iff a value with the given schema can be used to populate
  /// 'slot_desc', as if 'schema' were the writer schema and 'slot_desc' the reader
  /// schema. 'schema' can be either a avro_schema_t or avro_datum_t.
  Status VerifyTypesMatch(SlotDescriptor* slot_desc, avro_obj_t* schema);

  /// Return true if reader_type can be used to read writer_type according to the Avro
  /// type promotion rules. Note that this does not handle nullability or TYPE_NULL.
  bool VerifyTypesMatch(const ColumnType& reader_type, const ColumnType& writer_type);

  /// Writes 'default_value' to 'slot_desc' in the template tuple, initializing the
  /// template tuple if it doesn't already exist. Returns a non-OK status if slot_desc's
  /// and default_value's types are incompatible or unsupported. field_name is used for
  /// error messages.
  Status WriteDefaultValue(SlotDescriptor* slot_desc, avro_datum_t default_value,
      const char* field_name);

  /// Decodes records and copies the data into tuples.
  /// Returns the number of tuples to be committed.
  /// - max_tuples: the maximum number of tuples to write
  /// - data: serialized record data. Is advanced as records are read.
  /// - pool: memory pool to allocate string data from
  /// - tuple: tuple pointer to copy objects to
  /// - tuple_row: tuple row of written tuples
  int DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data,
      Tuple* tuple, TupleRow* tuple_row);

  /// Materializes a single tuple from serialized record data.
  void MaterializeTuple(const AvroSchemaElement& record_schema, MemPool* pool,
      uint8_t** data, Tuple* tuple);

  /// Produces a version of DecodeAvroData that uses codegen'd instead of interpreted
  /// functions.
  static llvm::Function* CodegenDecodeAvroData(
      RuntimeState* state, llvm::Function* materialize_tuple_fn,
      const std::vector<ExprContext*>& conjunct_ctxs);

  /// Codegens a version of MaterializeTuple() that reads records based on the table
  /// schema.
  /// TODO: Codegen a function for each unique file schema.
  static llvm::Function* CodegenMaterializeTuple(HdfsScanNode* node,
                                                 LlvmCodeGen* codegen);

  /// Used by CodegenMaterializeTuple to recursively create the IR for reading an Avro
  /// record.
  /// - path: the column path constructed so far. This is used to find the slot desc, if
  ///     any, associated with each field of the record. Note that this assumes the
  ///     table's Avro schema matches up with the table's column definitions by ordinal.
  /// - builder: used to insert the IR, starting at the current insert point. The insert
  ///     point will be left at the end of the record but before the 'insert_before'
  ///     block.
  /// - insert_before: the block to insert any new blocks directly before. NULL if blocks
  ///     should be inserted at the end of fn. (This could theoretically be inferred from
  ///     builder's insert point, but I can't figure out how to get the successor to a
  ///     basic block.)
  /// - this_val, pool_val, tuple_val, data_val: arguments to MaterializeTuple()
  static Status CodegenReadRecord(
      const SchemaPath& path, const AvroSchemaElement& record, HdfsScanNode* node,
      LlvmCodeGen* codegen, void* builder, llvm::Function* fn,
      llvm::BasicBlock* insert_before, llvm::Value* this_val, llvm::Value* pool_val,
      llvm::Value* tuple_val, llvm::Value* data_val);

  /// Creates the IR for reading an Avro scalar at builder's current insert point.
  static Status CodegenReadScalar(const AvroSchemaElement& element,
    SlotDescriptor* slot_desc, LlvmCodeGen* codegen, void* builder, llvm::Value* this_val,
    llvm::Value* pool_val, llvm::Value* tuple_val, llvm::Value* data_val);

  /// The following are cross-compiled functions for parsing a serialized Avro primitive
  /// type and writing it to a slot. They can also be used for skipping a field without
  /// writing it to a slot by setting 'write_slot' to false.
  /// - data: Serialized record data. Is advanced past the read field.
  /// The following arguments are used only if 'write_slot' is true:
  /// - slot: The tuple slot to write the parsed field into.
  /// - type: The type of the slot. (This is necessary because there is not a 1:1 mapping
  ///         between Avro types and Impala's primitive types.)
  /// - pool: MemPool for string data.
  void ReadAvroBoolean(
      PrimitiveType type, uint8_t** data, bool write_slot, void* slot, MemPool* pool);
  void ReadAvroInt32(
      PrimitiveType type, uint8_t** data, bool write_slot, void* slot, MemPool* pool);
  void ReadAvroInt64(
      PrimitiveType type, uint8_t** data, bool write_slot, void* slot, MemPool* pool);
  void ReadAvroFloat(
      PrimitiveType type, uint8_t** data, bool write_slot, void* slot, MemPool* pool);
  void ReadAvroDouble(
      PrimitiveType type, uint8_t** data, bool write_slot, void* slot, MemPool* pool);
  void ReadAvroVarchar(
      PrimitiveType type, int max_len, uint8_t** data, bool write_slot, void* slot,
      MemPool* pool);
  void ReadAvroChar(
      PrimitiveType type, int max_len, uint8_t** data, bool write_slot, void* slot,
      MemPool* pool);
  void ReadAvroString( PrimitiveType type, uint8_t** data, bool write_slot, void* slot,
      MemPool* pool);

  /// Same as the above functions, except takes the size of the decimal slot (i.e. 4, 8, or
  /// 16) instead of the type (which should be TYPE_DECIMAL). The slot size is passed
  /// explicitly, rather than passing a ColumnType, so we can easily pass in a constant in
  /// the codegen'd MaterializeTuple() function. If 'write_slot' is false, 'slot_byte_size'
  /// is ignored.
  void ReadAvroDecimal(
      int slot_byte_size, uint8_t** data, bool write_slot, void* slot, MemPool* pool);

  /// Reads and advances 'data' past the union branch index and returns true if the
  /// corresponding element is non-null. 'null_union_position' must be 0 or 1.
  bool ReadUnionType(int null_union_position, uint8_t** data);

  static const char* LLVM_CLASS_NAME;
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

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

// This scanner reads Avro object container files (i.e., Avro data files)
// located in HDFS and writes the content as tuples in the Impala in-memory
// representation of data (e.g. tuples, rows, row batches).
//
// The specification for Avro files can be found at
// http://avro.apache.org/docs/current/spec.html (the current Avro version is
// 1.7.4 as of the time of this writing). At a high level, an Avro data file has
// the following structure:
//
// - Avro data file
//   - file header
//     - file version header
//     - file metadata
//       - JSON schema
//       - compression codec (optional)
//     - sync marker
//   - data block+
//     - # of Avro objects in block
//     - size of objects in block (post-compression)
//     - serialized objects
//     - sync marker
//
//
// This implementation reads one data block at a time, using the schema from the file
// header to decode the serialized objects. If possible, non-materialized columns are
// skipped without being read. If codegen is enabled, we codegen a function based on the
// table schema that parses records, materializes them to tuples, and evaluates the
// conjuncts.
//
// The Avro C library is used to parse the file's schema and the table's schema, which are
// then resolved according to the Avro spec and transformed into our own schema
// representation (i.e. a list of SchemaElements). Schema resolution allows users to
// evolve the table schema and file schema(s) independently. The spec goes over all the
// rules for schema resolution, but in summary:
//
// - Record fields are matched by name (and thus can be reordered; the table schema
//   determines the order of the columns)
// - Fields in the file schema not present in the table schema are ignored
// - Fields in the table schema not present in the file schema must have a default value
//   specified
// - Types can be "promoted" as follows:
//   int -> long -> float -> double
//
// TODO:
// - implement SkipComplex()
// - codegen a function per unique file schema, rather than just the table schema
// - once Exprs are thread-safe, we can cache the jitted function directly
// - microbenchmark codegen'd functions (this and other scanners)

#include "exec/base-sequence-scanner.h"

#include <avro/basics.h>
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

// From avro/schema.h
struct avro_obj_t;
typedef struct avro_obj_t* avro_schema_t;

namespace impala {

class HdfsAvroScanner : public BaseSequenceScanner {
 public:
  // The four byte Avro version header present at the beginning of every
  // Avro file: {'O', 'b', 'j', 1}
  static const uint8_t AVRO_VERSION_HEADER[4];

  HdfsAvroScanner(HdfsScanNode* scan_node, RuntimeState* state);

  // Codegen parsing records, writing tuples and evaluating predicates.
  static llvm::Function* Codegen(HdfsScanNode*,
                                 const std::vector<ExprContext*>& conjunct_ctxs);

 protected:
  // Implementation of BaseSeqeunceScanner super class methods
  virtual FileHeader* AllocateFileHeader();
  // TODO: check that file schema matches metadata schema
  virtual Status ReadFileHeader();
  virtual Status InitNewRange();
  virtual Status ProcessRange();

  virtual THdfsFileFormat::type file_format() const {
    return THdfsFileFormat::AVRO;
  }

 private:
  // Wrapper for avro_schema_t's that handles decrementing the ref count
  struct ScopedAvroSchemaT {
    ScopedAvroSchemaT(avro_schema_t s = NULL) : schema(s) { }
    ScopedAvroSchemaT(const ScopedAvroSchemaT&);

    ~ScopedAvroSchemaT();

    ScopedAvroSchemaT& operator=(const ScopedAvroSchemaT&);
    ScopedAvroSchemaT& operator=(const avro_schema_t& s);

    avro_schema_t operator->() const { return schema; }
    avro_schema_t get() const { return schema; }

   private:
    // If not NULL, this owns a reference to schema
    avro_schema_t schema;
  };

  // Internal representation of a field/column schema. The schema for a data file is a
  // record with a field for each column. For a given file, we create a SchemaElement for
  // each field of the record.
  struct SchemaElement {
    // The record field schema from the file.
    ScopedAvroSchemaT schema;

    // Complex types, e.g. records, may have nested child types
    std::vector<SchemaElement> children;

    // Avro supports nullable types via unions of the form [<type>, "null"]. We special
    // case nullable primitives by storing which position "null" occupies in the union and
    // setting 'schema' to the non-null type's schema, rather than the union schema.
    // null_union_position is set to 0 or 1 accordingly if this type is a union between a
    // primitive type and "null", and -1 otherwise.
    int null_union_position;

    // The slot descriptor corresponding to this element. NULL if this element does not
    // correspond to a materialized column.
    const SlotDescriptor* slot_desc;
  };

  struct AvroFileHeader : public BaseSequenceScanner::FileHeader {
    // List of SchemaElements corresponding to the fields of the file schema.
    std::vector<SchemaElement> schema;

    // Template tuple for this file containing partition key values and default values.
    // NULL if there are no materialized partition keys and no default values are
    // necessary (i.e., all materialized fields are present in the file schema).
    // template_tuple_ is set to this value.
    Tuple* template_tuple;

    // True if this file can use the codegen'd version of DecodeAvroData() (i.e. its
    // schema matches the table schema), false otherwise.
    bool use_codegend_decode_avro_data;
  };

  AvroFileHeader* avro_header_;

  // Metadata keys
  static const std::string AVRO_SCHEMA_KEY;
  static const std::string AVRO_CODEC_KEY;

  // Supported codecs, as they appear in the metadata
  static const std::string AVRO_NULL_CODEC;
  static const std::string AVRO_SNAPPY_CODEC;
  static const std::string AVRO_DEFLATE_CODEC;

  typedef int (*DecodeAvroDataFn)(HdfsAvroScanner*, int, MemPool*, uint8_t**,
                                  Tuple*, TupleRow*);

  // The codegen'd version of DecodeAvroData() if available, NULL otherwise.
  DecodeAvroDataFn codegend_decode_avro_data_;

  // Utility function for decoding and parsing file header metadata
  Status ParseMetadata();

  // Populates avro_header_->schema with the result of resolving the the table's schema
  // with the file's schema. Default values are written to avro_header_->template_tuple
  // (which is initialized to template_tuple_ if necessary).
  Status ResolveSchemas(const avro_schema_t& table_root,
                        const avro_schema_t& file_root);

  // Utility function that maps the Avro library's type representation to our own.
  static SchemaElement ConvertSchema(const avro_schema_t& schema);

  // Returns Status::OK iff a value with the given schema can be used to populate
  // slot_desc. 'schema' can be either a avro_schema_t or avro_datum_t.
  Status VerifyTypesMatch(SlotDescriptor* slot_desc, avro_obj_t* schema);

  // Decodes records and copies the data into tuples.
  // Returns the number of tuples to be committed.
  // - max_tuples: the maximum number of tuples to write
  // - data: serialized record data. Is advanced as records are read.
  // - pool: memory pool to allocate string data from
  // - tuple: tuple pointer to copy objects to
  // - tuple_row: tuple row of written tuples
  int DecodeAvroData(int max_tuples, MemPool* pool, uint8_t** data,
                        Tuple* tuple, TupleRow* tuple_row);

  // Materializes a single tuple from serialized record data.
  void MaterializeTuple(MemPool* pool, uint8_t** data, Tuple* tuple);

  // Produces a version of DecodeAvroData that uses codegen'd instead of interpreted
  // functions.
  static llvm::Function* CodegenDecodeAvroData(
      RuntimeState* state, llvm::Function* materialize_tuple_fn,
      const std::vector<ExprContext*>& conjunct_ctxs);

  // Codegens a version of MaterializeTuple() that reads records based on the table
  // schema.
  // TODO: Codegen a function for each unique file schema.
  static llvm::Function* CodegenMaterializeTuple(HdfsScanNode* node,
                                                 LlvmCodeGen* codegen);

  // The following are cross-compiled functions for parsing a serialized Avro primitive
  // type and writing it to a slot. They can also be used for skipping a field without
  // writing it to a slot by setting 'write_slot' to false.
  // - data: Serialized record data. Is advanced past the read field.
  // The following arguments are used only if 'write_slot' is true:
  // - slot: The tuple slot to write the parsed field into.
  // - type: The type of the slot. (This is necessary because there is not a 1:1 mapping
  //         between Avro types and Impala's primitive types.)
  // - pool: MemPool for string data.
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

  // Same as the above functions, except takes the size of the decimal slot (i.e. 4, 8, or
  // 16) instead of the type (which should be TYPE_DECIMAL). The slot size is passed
  // explicitly, rather than passing a ColumnType, so we can easily pass in a constant in
  // the codegen'd MaterializeTuple() function. If 'write_slot' is false, 'slot_byte_size'
  // is ignored.
  void ReadAvroDecimal(
      int slot_byte_size, uint8_t** data, bool write_slot, void* slot, MemPool* pool);

  // Reads and advances 'data' past the union branch index and returns true if the
  // corresponding element is non-null. 'null_union_position' must be 0 or 1.
  bool ReadUnionType(int null_union_position, uint8_t** data);

  static const char* LLVM_CLASS_NAME;
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

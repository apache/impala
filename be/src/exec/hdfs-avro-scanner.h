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
// This implementation reads one data block at a time, using the schema from the
// file header to decode the serialized objects. If possible, non-materialized
// columns are skipped without being read.
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
// - codegen

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
  virtual ~HdfsAvroScanner();

 protected:
  // Implementation of BaseSeqeunceScanner super class methods
  virtual Status Prepare(ScannerContext* context);
  virtual FileHeader* AllocateFileHeader();
  // TODO: check that file schema matches metadata schema
  virtual Status ReadFileHeader();
  virtual Status InitNewRange();
  virtual Status ProcessRange();

  virtual THdfsFileFormat::type file_format() const {
    return THdfsFileFormat::AVRO;
  }

 private:
  // All types >= COMPLEX_TYPE are complex (nested) types
  static const avro_type_t COMPLEX_TYPE = AVRO_RECORD;

  struct SchemaElement {
    avro_type_t type;

    // Complex types, e.g. records, may have nested child types
    std::vector<SchemaElement> children;

    // Avro supports nullable types via unions of the form [<type>, "null"]. We
    // special case nullable primitives by storing which position "null"
    // occupies in the union and setting type to the primitive, rather than
    // UNION. null_union_position is set to 0 or 1 accordingly if this type is a
    // union between a primitive type and "null", and -1 otherwise.
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

    // Pool for holding default string values referenced by template_tuple.
    boost::scoped_ptr<MemPool> default_data_pool;
  };

  AvroFileHeader* avro_header_;

  // Metadata keys
  static const std::string AVRO_SCHEMA_KEY;
  static const std::string AVRO_CODEC_KEY;

  // Supported codecs, as they appear in the metadata
  static const std::string AVRO_NULL_CODEC;
  static const std::string AVRO_SNAPPY_CODEC;
  static const std::string AVRO_DEFLATE_CODEC;

  // Utility function for decoding and parsing file header metadata
  Status ParseMetadata();

  // Populates avro_header_->schema with the result of resolving the the table's schema
  // with the file's schema. Default values are written to avro_header_->template_tuple
  // (which is initialized to template_tuple_ if necessary).
  Status ResolveSchemas(const avro_schema_t& table_root,
                        const avro_schema_t& file_root);

  // Utility function that maps the Avro library's type representation to our
  // own. Used to convert a ValidSchema to a vector of SchemaElements.
  SchemaElement ConvertSchemaNode(const avro_schema_t& node);

  // Returns Status::OK iff a value of avro_type can be used to populate slot_desc.
  Status VerifyTypesMatch(SlotDescriptor* slot_desc, avro_type_t avro_type);

  // Decodes records, copies the data into tuples, and commits the tuple rows.
  // - max_tuples: the maximum number of tuples to write and commit
  // - num_records: the number of records remaining in this data block. This is
  //       decremented by the number of records decoded.
  // - data: serialized record data. Is advanced as records are read.
  // - data_len: the length of data. Is decremented as records are read.
  // - pool: memory pool to allocate string data from
  // - tuple: tuple pointer to copy objects to
  // - tuple_row: tuple row of written tuples
  Status DecodeAvroData(int max_tuples, int64_t* num_records, MemPool* pool,
                        uint8_t** data, int* data_len, Tuple* tuple, TupleRow* tuple_row);

  // Read the primitive type 'element' from 'data' and write it to the slot in 'tuple'
  // specified by 'slot_desc'. String data is allocated from pool if necessary. 'data' is
  // advanced past the element read and 'data_len' is decremented appropriately if there
  // is no error. Returns true if no error.
  bool ReadPrimitive(const SchemaElement& element, const SlotDescriptor& slot_desc,
                     MemPool* pool, uint8_t** data, int* data_len, Tuple* tuple);

  // Advance 'data' past the primitive type 'element' and decrement 'data_len'
  // appropriately. Avoids reading 'data' when possible. Returns true if no error.
  bool SkipPrimitive(const SchemaElement& element, uint8_t** data, int* data_len);

  // Advance 'data' past the complex type 'element' and decrement 'data_len'
  // appropriately. Avoids reading 'data' when possible. Returns true if no error. By
  // skipping complex types, we can execute queries over tables with nested data types if
  // none of those columns are materialized.
  // TODO: implement this function
  bool SkipComplex(const SchemaElement& element, uint8_t** data, int* data_len);

  // Utility function that uses element.null_union_position to set 'type' to the next
  // primitive type we should read from 'data'.
  bool ReadUnionType(const SchemaElement& element, uint8_t** data, int* data_len,
                     avro_type_t* type);
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

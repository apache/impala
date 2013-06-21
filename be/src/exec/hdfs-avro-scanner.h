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
// 1.7.3 as of the time of this writing). Also see DataFile.hh/cc in the Avro
// C++ library. At a high level, an Avro data file has the following structure:
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
// The Avro C++ library is used to parse the file's schema and the table's schema into
// ValidSchema objects, which are then resolved according to the Avro spec and transformed
// into our own schema representation. Schema resolution allows users to evolve the table
// schema and file schema(s) independently. The spec goes over all the rules for schema
// resolution, but in summary:
//
// - Record fields are matched by name (and thus can be reordered; the table schema
//   determines the order of the columns)
// - Fields in the file schema not present in the table schema are ignored
// - Fields in the table schema not present in the file schema must have a default value
//   specified (not yet implemented)
// - Types can be "promoted" as follows:
//   int -> long -> float -> double (not yet implemented)
//
// TODO:
// - implement SkipComplex()
// - codegen
// - default field values
// - type promotion

#include "exec/base-sequence-scanner.h"

#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace avro {
  class Node;
  class ValidSchema;
}

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
  struct SchemaElement {
    enum Type {
      NULL_TYPE,
      BOOLEAN,
      INT,
      LONG,
      FLOAT,
      DOUBLE,
      BYTES,
      STRING,
      COMPLEX_TYPE, // marker dividing primitive from complex (nested) types
      RECORD,
      ENUM,
      ARRAY,
      MAP,
      UNION,
      FIXED,
    } type;

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
    std::vector<SchemaElement> schema;
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
  // with the file's schema.
  Status ResolveSchemas(const avro::ValidSchema& table_schema,
                        const avro::ValidSchema& file_schema);

  // Utility function that maps the Avro library's type representation to our
  // own. Used to convert a ValidSchema to a vector of SchemaElements.
  SchemaElement ConvertSchemaNode(const avro::Node& node);

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
                     SchemaElement::Type* type);
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

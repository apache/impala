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

// This implementation reads one data block at a time, using the Avro decoder
// provided in the Avro C++ library to decode the serialized objects (the
// decoder doesn't read from the scan range context directly -- the serialized
// blob is read from the context and passed into the decoder via Avro's
// MemoryInputStream).
//
// TODO:
// - codegen
// - super high performance Avro decoder

#include "exec/base-sequence-scanner.h"

// TODO: figure out how to forward declare this
#include <ValidSchema.hh>
#include "exec/scanner-context.h"
#include "exec/serde-utils.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace avro {
  class Decoder;
  class GenericDatum;
  class GenericRecord;
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
  // Implementation of sequence container super class methods
  virtual FileHeader* AllocateFileHeader();
  // TODO: check that file schema matches metadata schema
  virtual Status ReadFileHeader();
  virtual Status InitNewRange();
  virtual Status ProcessRange();
  
  virtual THdfsFileFormat::type file_format() const { 
    return THdfsFileFormat::AVRO; 
  }

 private:
  struct AvroFileHeader : public BaseSequenceScanner::FileHeader {
    avro::ValidSchema schema;
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

  // Decodes records, copies the data into tuples, and commits the tuple rows.
  // - decoder: an avro decoder that is initialized to a stream and ready to
  //       decode records
  // - datum: datum to read records into (passed in so we only have to
  //       initialize once). Note that although a GenericDatum can be of any
  //       type, we're expecting only records.
  // - pool: memory pool to allocate string data from
  // - tuple: tuple pointer to copy objects to
  // - tuple_row: tuple row of written tuples
  // - max_tuples: the maximum number of tuples to write and commit
  // - records_remaining: the maximum number of records that decoder can decode
  //       before its input stream runs out. This is decremented by the number
  //       of records decoded.
  Status DecodeAvroData(avro::Decoder* decoder, avro::GenericDatum* datum,
                        MemPool* pool, Tuple* tuple, TupleRow* tuple_row,
                        int max_tuples, int64_t* records_remaining);

  // Reads an Avro record and copies the data to tuple, allocating any string
  // data from pool.
  inline Status ReadRecord(const avro::GenericRecord& record,
                           MemPool* pool, Tuple* tuple);
};
} // namespace impala

#endif // IMPALA_EXEC_HDFS_AVRO_SCANNER_H

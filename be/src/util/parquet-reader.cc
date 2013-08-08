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

#include <iostream>
#include <sstream>
#include <vector>
#include <gflags/gflags.h>
#include <snappy.h>
#include "gen-cpp/parquet_types.h"

// TCompactProtocol requires some #defines to work right.
// TODO: is there a better include to use?
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wstring-plus-int"
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>
#include <thrift/transport/TBufferTransports.h>
#pragma clang diagnostic pop

#include "util/rle-encoding.h"

DEFINE_string(file, "", "File to read");
DEFINE_bool(output_page_header, false, "If true, output page headers to stderr.");

using namespace boost;
using namespace parquet;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

// Some code is replicated to make this more stand-alone.
const uint8_t PARQUET_VERSION_NUMBER[] = {'P', 'A', 'R', '1'};

shared_ptr<TProtocol> CreateDeserializeProtocol(
    shared_ptr<TMemoryBuffer> mem, bool compact) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  }
}

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
bool DeserializeThriftMsg(uint8_t* buf, uint32_t* len, bool compact,
    T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  shared_ptr<TMemoryBuffer> tmem_transport(new TMemoryBuffer(buf, *len));
  shared_ptr<TProtocol> tproto = CreateDeserializeProtocol(tmem_transport, compact);
  try {
    deserialized_msg->read(tproto.get());
  } catch (apache::thrift::protocol::TProtocolException& e) {
    cerr << "couldn't deserialize thrift msg:\n" << e.what() << endl;
    return false;
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
  return true;
}

string TypeMapping(Type::type t) {
  switch (t) {
    case Type::BOOLEAN:
      return "BOOLEAN";
    case Type::INT32:
      return "INT32";
    case Type::INT64:
      return "INT64";
    case Type::FLOAT:
      return "FLOAT";
    case Type::DOUBLE:
      return "DOUBLE";
    case Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
    default:
      return "UNKNOWN";
  }
}

void AppendSchema(const vector<SchemaElement>& schema, int level,
    int* idx, stringstream* ss) {
  for (int i = 0; i < level; ++i) {
    (*ss) << "  ";
  }
  (*ss) << schema[*idx].name;
  if (schema[*idx].__isset.type) (*ss) << "  " << TypeMapping(schema[*idx].type);
  (*ss) << endl;

  int num_children = schema[*idx].num_children;
  ++*idx;
  for (int i = 0; i < num_children; ++i) {
    AppendSchema(schema, level + 1, idx, ss);
  }
}

string GetSchema(const FileMetaData& md) {
  const vector<SchemaElement>& schema = md.schema;
  if (schema.empty()) return "Invalid schema";
  int num_root_elements = schema[0].num_children;
  stringstream ss;
  int idx = 1;
  for (int i = 0; i < num_root_elements; ++i) {
    AppendSchema(schema, 0, &idx, &ss);
  }
  return ss.str();
}

// Simple utility to read parquet files on local disk.  This utility validates the
// file is correctly formed and can output values from each data page.  The
// entire file is buffered in memory so this is not suitable for very large files.
// We expect the table to be split into multiple parquet files, each the size of
// a HDFS block, in which case this utility will be able to able to handle it.
// cerr is used to output diagnostics of the file
// cout is used to output converted data (in csv)
int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_file.size() == 0) {
    cout << "Must specify input file." << endl;
    return -1;
  }

  FILE* file = fopen(FLAGS_file.c_str(), "r");
  assert(file != NULL);

  fseek(file, 0L, SEEK_END);
  size_t file_len = ftell(file);
  fseek(file, 0L, SEEK_SET);

  cerr << "File Length: " << file_len << endl;

  uint8_t* buffer = reinterpret_cast<uint8_t*>(malloc(file_len));
  size_t bytes_read = fread(buffer, 1, file_len, file);
  assert(bytes_read == file_len);

  // Check file starts and ends with magic bytes
  assert(
      memcmp(buffer, PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)) == 0);
  assert(memcmp(buffer + file_len - sizeof(PARQUET_VERSION_NUMBER),
      PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)) == 0);

  // Get metadata
  uint8_t* metadata_len_ptr =
      buffer + file_len - sizeof(PARQUET_VERSION_NUMBER) - sizeof(uint32_t);
  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(metadata_len_ptr);
  cerr << "Metadata len: " << metadata_len << endl;

  uint8_t* metadata = metadata_len_ptr - metadata_len;

  FileMetaData file_metadata;
  bool status = DeserializeThriftMsg(metadata, &metadata_len, true, &file_metadata);
  assert(status);
  cerr << ThriftDebugString(file_metadata) << endl;
  cerr << "Schema: " << endl << GetSchema(file_metadata) << endl;

  int pages_skipped = 0;
  int pages_read = 0;
  int num_rows = 0;
  int total_page_header_size = 0;
  int total_compressed_data_size = 0;
  int total_uncompressed_data_size = 0;
  vector<int> column_sizes;

  // Buffer to decompress data into.  Reused across pages.
  vector<char> decompression_buffer;

  for (int i = 0; i < file_metadata.row_groups.size(); ++i) {
    cerr << "Reading row group " << i << endl;
    RowGroup& rg = file_metadata.row_groups[i];
    column_sizes.resize(rg.columns.size());

    for (int c = 0; c < rg.columns.size(); ++c) {
      cerr << "  Reading column " << c << endl;
      ColumnChunk& col = rg.columns[c];

      int first_page_offset = col.meta_data.data_page_offset;
      if (col.meta_data.__isset.dictionary_page_offset) {
        first_page_offset = ::min(first_page_offset,
            (int)col.meta_data.dictionary_page_offset);
      }
      uint8_t* data = buffer + first_page_offset;
      uint8_t* col_end = data + col.meta_data.total_compressed_size;

      // Loop through the entire column chunk.  This lets us walk all the pages.
      while (data < col_end) {
        uint32_t header_size = file_len - col.file_offset;
        PageHeader header;
        status = DeserializeThriftMsg(data, &header_size, true, &header);
        assert(status);
        if (FLAGS_output_page_header) {
          cerr << ThriftDebugString(header) << endl;
        }

        data += header_size;
        total_page_header_size += header_size;
        column_sizes[c] += header.compressed_page_size;
        total_compressed_data_size += header.compressed_page_size;
        total_uncompressed_data_size += header.uncompressed_page_size;
        data += header.compressed_page_size;
        ++pages_read;
      }
      // Check that we ended exactly where we should have
      assert(data == col_end);
    }
  }
  double compression_ratio =
      (double)total_uncompressed_data_size / total_compressed_data_size;
  stringstream ss;
  ss << "\nSummary:\n"
     << "  Rows: " << num_rows << endl
     << "  Read pages: " << pages_read << endl
     << "  Skipped pages: " << pages_skipped << endl
     << "  Metadata size: " << metadata_len
     << "(" << (metadata_len / (double)file_len) << ")" << endl
     << "  Total page header size: " << total_page_header_size
     << "(" << (total_page_header_size / (double)file_len) << ")" << endl;
  ss << "  Column compressed size: " << total_compressed_data_size
     << "(" << (total_compressed_data_size / (double)file_len) << ")" << endl;
  ss << "  Column uncompressed size: " << total_uncompressed_data_size
     << "(" << compression_ratio << ")" << endl;
  for (int i = 0; i < column_sizes.size(); ++i) {
    ss << "    " << "Col " << i << ": " << column_sizes[i]
       << "(" << (column_sizes[i] / (double)file_len) << ")" << endl;
  }
  cerr << ss.str() << endl;

  return 0;
}

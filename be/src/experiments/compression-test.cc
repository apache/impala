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

#include <string>
#include <gtest/gtest.h>

#include "util/compress.h"

#include "common/names.h"

namespace impala {

// Utility benchmark to test how well we can compress random string data.
// NumStrings=1000000 MinLen=10 MaxLen=10 Codec=SNAPPY
//   Uncompressed len: 10000000
//   Compressed len: 10006377
//   Sorted Compressed len: 9346971
// NumStrings=1000000 MinLen=10 MaxLen=10 Codec=GZIP
//   Uncompressed len: 10000000
//   Compressed len: 6352396
//   Sorted Compressed len: 5712650
// NumStrings=1000000 MinLen=5 MaxLen=15 Codec=SNAPPY
//   Uncompressed len: 9498531
//   Compressed len: 9503924
//   Sorted Compressed len: 8825841
// NumStrings=1000000 MinLen=5 MaxLen=15 Codec=GZIP
//   Uncompressed len: 9497973
//   Compressed len: 6033310
//   Sorted Compressed len: 5429661

// Generates num strings between min_len and max_len.
// Outputs the uncompressed/compressed/sorted_compressed sizes.
void TestCompression(int num, int min_len, int max_len, THdfsCompression::type format) {
  vector<string> strings;
  uint8_t* buffer = (uint8_t*)malloc(max_len * num);
  int offset = 0;
  int len_delta = max_len - min_len;
  len_delta = max(len_delta, 1);
  for (int i = 0; i < num; ++i) {
    int len = rand() % len_delta + min_len;
    int start = offset;
    for (int j = 0; j < len; ++j) {
      buffer[offset++] = rand() % 26 + 'a';
    }
    strings.push_back(string((char*)buffer + start, len));
  }

  // Sort the input and make a new buffer
  uint8_t* sorted_buffer = (uint8_t*)malloc(offset);
  int sorted_offset = 0;
  sort(strings.begin(), strings.end());
  for (int i = 0; i < strings.size(); ++i) {
    memcpy(sorted_buffer + sorted_offset, strings[i].data(), strings[i].size());
    sorted_offset += strings[i].size();
  }

  scoped_ptr<Codec> compressor;
  Codec::CodecInfo codec_info(format);
  Status status = Codec::CreateCompressor(NULL, false, codec_info, &compressor);
  DCHECK(status.ok());

  int64_t compressed_len = compressor->MaxOutputLen(offset);
  uint8_t* compressed_buffer = (uint8_t*)malloc(compressed_len);
  ABORT_IF_ERROR(
      compressor->ProcessBlock(true, offset, buffer, &compressed_len, &compressed_buffer));

  int64_t sorted_compressed_len = compressor->MaxOutputLen(offset);
  uint8_t* sorted_compressed_buffer = (uint8_t*)malloc(sorted_compressed_len);
  ABORT_IF_ERROR(compressor->ProcessBlock(true, offset, sorted_buffer,
        &sorted_compressed_len, &sorted_compressed_buffer));

  cout << "NumStrings=" << num << " MinLen=" << min_len << " MaxLen=" << max_len
       << " Codec=" << codec_info.format_ << endl;
  cout << "  Uncompressed len: " << offset << endl;
  cout << "  Compressed len: " << compressed_len << endl;
  cout << "  Sorted Compressed len: " << sorted_compressed_len << endl;

  compressor->Close();
  free(buffer);
  free(compressed_buffer);
  free(sorted_buffer);
  free(sorted_compressed_buffer);
}

}

int main(int argc, char **argv) {
  impala::TestCompression(1000000, 10, 10, impala::THdfsCompression::SNAPPY);
  impala::TestCompression(1000000, 10, 10, impala::THdfsCompression::GZIP);
  impala::TestCompression(1000000, 5, 15, impala::THdfsCompression::SNAPPY);
  impala::TestCompression(1000000, 5, 15, impala::THdfsCompression::GZIP);
  return 0;
}

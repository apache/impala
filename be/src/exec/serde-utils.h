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


#ifndef IMPALA_EXEC_SERDE_UTILS_H_
#define IMPALA_EXEC_SERDE_UTILS_H_

#include <vector>
#include "common/status.h"

namespace impala {

// SerDeUtils:
// A collection of utility functions for deserializing data from buffers.
class SerDeUtils {
 public:
  static const int MAX_VINT_LEN = 9;

  // Get a big endian integer from a buffer.  The buffer does not have to be word aligned.
  static int32_t GetInt(const uint8_t* buffer);
  static int16_t GetSmallInt(const uint8_t* buffer);
  static int64_t GetLongInt(const uint8_t* buffer);

  // Put an Integer into a buffer in big endian order .  The buffer must be at least
  // 4 bytes long.
  static void PutInt(uint8_t* buf, int32_t integer);

  // Get a variable-length Long or int value from a byte buffer.
  // Returns the length of the long/int
  // If the size byte is corrupted then return -1;
  static int GetVLong(uint8_t* buf, int64_t* vlong);
  static int GetVInt(uint8_t* buf, int32_t* vint);

  // Read a variable-length Long value from a byte buffer
  // starting at the specified byte offset.
  static int GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong);

  // Dump the first length bytes of buf to a Hex string.
  static std::string HexDump(const uint8_t* buf, int64_t length);
  static std::string HexDump(const char* buf, int64_t length);

  // Determines the sign of a VInt/VLong from the first byte.
  static bool IsNegativeVInt(int8_t byte);

  // Determines the total length in bytes of a Writable VInt/VLong
  // from the first byte.
  static int DecodeVIntSize(int8_t byte);
};

} // namespace impala

#endif // IMPALA_EXEC_SERDE_UTIL

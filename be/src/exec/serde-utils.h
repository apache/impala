// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SERDE_UTILS_H_
#define IMPALA_EXEC_SERDE_UTILS_H_

#include <vector>
#include "common/status.h"

namespace impala {

class ByteStream;
class ScanRangeContext;

// SerDeUtils:
// A collection of utility functions for deserializing
// data written using either standard Java serialization
// or Hadoop Writables.
//
// Ref: http://download.oracle.com/javase/6/docs/api/java/io/DataInput.html
// Ref: http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html
//
// There are 3 version of some of the serde utilities.
//  1. The buffer is known to be big enough, simply parse it for the value.
//  2. The buffer if read from scan range context.  This blocks and waits for more
//     bytes as needed (bytes are provided asynchronously by another thread).
//  3. The operation is over a byte stream and the function will read more bytes as
//     needed (synchronously).  TODO: this should be removed.
class SerDeUtils {
 public:
  static const int MAX_VINT_LEN = 9;

  // Get an Integer from a buffer.  The buffer does not have to be word aligned.
  static int32_t GetInt(const uint8_t* buffer);

  // Put an Integer into a buffer in "Hadoop format".  The buffer must be at least
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

  // Case 2 functions
  static bool ReadBoolean(ScanRangeContext* context, bool* boolean, Status*);
  static bool ReadInt(ScanRangeContext* context, int32_t* val, Status*);
  static bool ReadVLong(ScanRangeContext* context, int64_t* val, Status*);
  static bool ReadVInt(ScanRangeContext* context, int32_t* val, Status*);
  static bool ReadBytes(ScanRangeContext* context, int length, uint8_t** buf, Status*);
  static bool SkipBytes(ScanRangeContext* context, int length, Status*);
  static bool ReadText(ScanRangeContext* context, uint8_t** buf, int* length, Status*);
  static bool SkipText(ScanRangeContext* context, Status*);

  // Read a Boolean primitive value written using Java serialization.
  // Equivalent to java.io.DataInput.readBoolean()
  static Status ReadBoolean(ByteStream* byte_stream, bool* boolean);

  // Read an Integer primitive value written using Java serialization.
  // Equivalent to java.io.DataInput.readInt()
  static Status ReadInt(ByteStream* byte_stream, int32_t* integer);

  // Read a variable-length Long value written using Writable serialization.
  // Ref: org.apache.hadoop.io.WritableUtils.readVLong()
  static Status ReadVLong(ByteStream* byte_stream, int64_t* vlong);

  // Read a variable length Integer value written using Writable serialization.
  // Ref: org.apache.hadoop.io.WritableUtils.readVInt()
  static Status ReadVInt(ByteStream* byte_stream, int32_t* vint);

  // Read length bytes from an HDFS file into the supplied buffer.
  static Status ReadBytes(ByteStream* byte_stream, int64_t length,
      std::vector<uint8_t>* buf);

  static Status ReadBytes(ByteStream* byte_stream, int64_t length,
      uint8_t* buf);

  // Skip over the next length bytes in the specified HDFS file.
  static Status SkipBytes(ByteStream* byte_stream, int64_t length);

  // Read a Writable Text value from the supplied file.
  // Ref: org.apache.hadoop.io.WritableUtils.readString()
  static Status ReadText(ByteStream* byte_stream, std::vector<char>* text);

  // Skip this text object.
  static Status SkipText(ByteStream* byte_stream);

  // Dump the first length bytes of buf to a Hex string.
  static std::string HexDump(const uint8_t* buf, int64_t length);
  static std::string HexDump(const char* buf, int64_t length);

 private:
  // Determines the sign of a VInt/VLong from the first byte.
  static bool IsNegativeVInt(int8_t byte);

  // Determines the total length in bytes of a Writable VInt/VLong
  // from the first byte.
  static int DecodeVIntSize(int8_t byte);
};

} // namespace impala

#endif // IMPALA_EXEC_SERDE_UTIL

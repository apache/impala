// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_SERDE_UTILS_H_
#define IMPALA_EXEC_SERDE_UTILS_H_

#include <vector>
#include <sstream>

#include <hdfs.h>

#include "common/status.h"
#include "exec/byte-stream.h"

namespace impala {

class Status;

// SerDeUtils:
// A collection of utility functions for deserializing
// data written using either standard Java serialization
// or Hadoop Writables.
//
// Ref: http://download.oracle.com/javase/6/docs/api/java/io/DataInput.html
// Ref: http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html
//
class SerDeUtils {
public:
  // Read a Boolean primitive value written using Java serialization.
  // Equivalent to java.io.DataInput.readBoolean()
  static Status ReadBoolean(ByteStream* byte_stream, bool* boolean);

  // Read an Integer primitive value written using Java serialization.
  // Equivalent to java.io.DataInput.readInt()
  static Status ReadInt(ByteStream* byte_stream, int32_t* integer);

  // Read an Integer from a buffer.
  static int32_t GetInt(uint8_t* buf) {
    return (buf[0] << 24)
        | (buf[1] << 16)
        | (buf[2] << 8)
        |  buf[3];
  }

  // Put an Integer into a buffer in "Hadoop format"
  static void PutInt(uint8_t* buf, int32_t integer) {
    buf[0] = integer >> 24;
    buf[1] = integer >> 16;
    buf[2] = integer >> 8;
    buf[3] = integer;
  }


  // Read a variable-length Long value written using Writable serialization.
  // Ref: org.apache.hadoop.io.WritableUtils.readVLong()
  static Status ReadVLong(ByteStream* byte_stream, int64_t* vlong);

  // Read a variable-length Long value from a byte buffer.
  static int GetVLong(uint8_t* buf, int64_t* vlong);

  // Read a variable-length Long value from a byte buffer
  // starting at the specified byte offset.
  static int GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong);

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

private:
  // Determines the sign of a VInt/VLong from the first byte.
  static bool IsNegativeVInt(int8_t byte);

  // Determines the total length in bytes of a Writable VInt/VLong
  // from the first byte.
  static int DecodeVIntSize(int8_t byte);
};

} // namespace impala

#endif // IMPALA_EXEC_SERDE_UTILS_H_

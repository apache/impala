// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_READ_WRITE_UTIL_H
#define IMPALA_EXEC_READ_WRITE_UTIL_H

#include "exec/hdfs-byte-stream.h"

class ByteStream;

namespace impala {

// Class for reading and writing various data types supported by Trevni and Avro.
class ReadWriteUtil {
 public:
  // Maximum lengths for Zigzag encodings.
  const static int MAX_ZINT_LEN = 5;
  const static int MAX_ZLONG_LEN = 10;

  // Read a Trevni string.
  static Status ReadString(ByteStream* byte_stream, std::string* str);

  // Read Trevni bytes.
  static Status ReadBytes(ByteStream* byte_stream, std::vector<uint8_t>* buf);

  // Read the length of the byte or string value and skip over it.
  static Status SkipBytes(ByteStream* byte_stream);

  // Return an integer from a buffer, stored little endian.
  static int32_t GetInt(uint8_t* buf) {
    return buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
  }

  // Return a long from a buffer, stored little endian.
  static int64_t GetLong(uint8_t* buf) {
    return buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24) |
        (static_cast<int64_t>(buf[4]) << 32) |
        (static_cast<int64_t>(buf[5]) << 40) |
        (static_cast<int64_t>(buf[6]) << 48) |
        (static_cast<int64_t>(buf[7]) << 56);
  }

  // Read an integer or long from the current byte stream.  Stored
  // little endian.
  template <typename T>
  static Status ReadInt(ByteStream* byte_stream, T* integer) {
    uint8_t buf[sizeof(T)];
    int64_t bytes_read;
    RETURN_IF_ERROR(byte_stream->Read(buf, sizeof(T), &bytes_read));
    if (bytes_read != sizeof(T)) {
      return Status("Short read in ReadInt");
    }
    if (sizeof(T) == sizeof(int32_t)) {
      *integer = GetInt(buf);
    } else {
      *integer = GetLong(buf);
    }
    return Status::OK;
  }

  // Get a zigzag encoded long integer from a buffer and return its length.
  // This is the integer encoding defined by google.com protocol-buffers:
  // https://developers.google.com/protocol-buffers/docs/encoding
  static int GetZLong(uint8_t* buf, int64_t* value) {
    uint64_t zlong = 0;
    int shift = 0;
    uint8_t* bp = buf;

    do {
      DCHECK_LE(shift, 64);
      zlong |= static_cast<uint64_t>(*bp & 0x7f) << shift;
      shift += 7;
    } while ((*(bp++) & 0x80) != 0);

    *value = (zlong >> 1) ^ -(zlong & 1);
    return bp - buf;
  }

  // Get a zigzag encoded integer from a buffer and return its length.
  static int GetZInt(uint8_t* buf, int32_t* integer) {
    uint32_t zint = 0;
    int shift = 0;
    uint8_t* bp = buf;

    do {
      DCHECK_LE(shift, 32);
      zint |= static_cast<uint32_t>(*bp & 0x7f) << shift;
      shift += 7;
    } while (*(bp++) & 0x80);

    *integer = (zint >> 1) ^ -(zint & 1);
    return bp - buf;
  }

  // Read a zigzag encoded integer from the current byte stream.
  static Status ReadZInt(ByteStream* byte_stream, int32_t* integer);

  // Read a zigzag encoded long integer from the current byte stream.
  static Status ReadZLong(ByteStream* byte_stream, int64_t* longint);

  // Put a zigzag encoded integer into a buffer and return its length.
  static int PutZInt(int32_t integer, uint8_t* buf);

  // Put a zigzag encoded long integer into a buffer and return its length.
  static int PutZLong(int64_t longint, uint8_t* buf);

  // Write a zigzag encoded integer to the output file.
  static Status WriteZInt(ByteStream* byte_stream, int32_t integer);

  // Write a zigzag encoded long integer to the output file.
  static Status WriteZLong(ByteStream* byte_stream, int64_t longint);

  // Put an integer into a buffer in little endian order.
  static void PutInt(int32_t integer, uint8_t* buf) {
    buf[0] = integer;
    buf[1] = integer >> 8;
    buf[2] = integer >> 16;
    buf[3] = integer >> 24;
  }

  // Put a long integer into a buffer in little endian order.
  static void PutLong(int64_t longint, uint8_t* buf) {
    buf[0] = longint;
    buf[1] = longint >> 8;
    buf[2] = longint >> 16;
    buf[3] = longint >> 24;
    buf[4] = longint >> 32;
    buf[5] = longint >> 40;
    buf[6] = longint >> 48;
    buf[7] = longint >> 56;
  }
};

}
#endif

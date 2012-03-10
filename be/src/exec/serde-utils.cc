// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/serde-utils.h"

#include <vector>
#include <hdfs.h>

#include "common/status.h"

using namespace std;
using namespace impala;

Status SerDeUtils::ReadBoolean(ByteStream* byte_stream, bool* boolean) {
  uint8_t byte;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream,
    sizeof(byte), reinterpret_cast<char*>(&byte)));
  if (byte != 0) {
    *boolean = true;
  } else {
    *boolean = false;
  }
  return Status::OK;
}

Status SerDeUtils::ReadInt(ByteStream* byte_stream, int32_t* integer) {
  uint8_t buf[sizeof(int)];
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, sizeof(int32_t),
    reinterpret_cast<char*>(&buf)));

  *integer =
      ((buf[0] & 0xff) << 24)
      | ((buf[1] & 0xff) << 16)
      | ((buf[2] & 0xff) << 8)
      |  (buf[3] & 0xff);
  return Status::OK;
}

Status SerDeUtils::ReadVLong(ByteStream* byte_stream, int64_t* vlong) {
  // Explanation from org.apache.hadoop.io.WritableUtils.writeVInt():
  // For -112 <= i <= 127, only one byte is used with the actual value.
  // For other values of i, the first byte value indicates whether the
  // long is positive or negative, and the number of bytes that follow.
  // If the first byte value v is between -113 and -120, the following long
  // is positive, with number of bytes that follow are -(v+112).
  // If the first byte value v is between -121 and -128, the following long
  // is negative, with number of bytes that follow are -(v+120). Bytes are
  // stored in the high-non-zero-byte-first order, AKA big-endian.
  int8_t firstbyte;
  uint8_t bytes[sizeof(int64_t)] = {0, 0, 0, 0, 0, 0, 0, 0};

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, sizeof(firstbyte),
    reinterpret_cast<char*>(&firstbyte)));

  int len = DecodeVIntSize(firstbyte);
  if (len == 1) {
    *vlong = (int64_t) firstbyte;
    return Status::OK;
  }
  --len;

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(byte_stream, len,
    reinterpret_cast<char*>(bytes)));

  *vlong &= ~*vlong;

  for (int i = 0; i < len; i++) {
    *vlong = (*vlong << 8) | (bytes[i] & 0xFF);
  }

  if (IsNegativeVInt(firstbyte)) {
    *vlong = *vlong ^ ((int64_t) - 1);
  }
  return Status::OK;
}

int SerDeUtils::ReadVLong(char* buf, int64_t* vlong) {
  return ReadVLong(buf, 0, vlong);
}

int SerDeUtils::ReadVLong(char* buf, int64_t offset, int64_t* vlong) {
  int8_t firstbyte = (int8_t) buf[0 + offset];

  int len = DecodeVIntSize(firstbyte);
  if (len == 1) {
    *vlong = (int64_t) firstbyte;
    return len;
  }

  *vlong &= ~*vlong;

  for (int i = 1; i < len; i++) {
    *vlong = (*vlong << 8) | (buf[i+offset] & 0xFF);
  }

  if (IsNegativeVInt(firstbyte)) {
    *vlong = *vlong ^ ((int64_t) - 1);
  }

  return len;
}

Status SerDeUtils::ReadVInt(ByteStream* byte_stream, int32_t* vint) {
  int64_t vlong;
  RETURN_IF_ERROR(ReadVLong(byte_stream, &vlong));
  *vint = (int32_t) vlong;
  return Status::OK;
}

Status SerDeUtils::ReadBytes(ByteStream* byte_stream, int64_t length,
                             std::vector<char>* buf) {
  buf->resize(length);
  int64_t actual_length = 0;
  RETURN_IF_ERROR(byte_stream->Read((&(*buf)[0]),
                                    length, &actual_length));
  if (length != actual_length) {
    return Status("EOF encountered while reading bytes");
  }
  return Status::OK;
}

Status SerDeUtils::ReadBytes(ByteStream* byte_stream, int64_t length,
                             char* buf) {
  int64_t actual_length = 0;
  RETURN_IF_ERROR(byte_stream->Read(buf,
                                    length, &actual_length));
  if (length != actual_length) {
    return Status("EOF encountered while reading bytes");
  }
  return Status::OK;
}


Status SerDeUtils::SkipBytes(ByteStream* byte_stream, int64_t length) {
  int64_t offset = 0;
  RETURN_IF_ERROR(byte_stream->GetPosition(&offset));
  RETURN_IF_ERROR(byte_stream->Seek(offset + length));
  return Status::OK;
}

Status SerDeUtils::ReadText(ByteStream* byte_stream, std::vector<char>* text) {
  int32_t length;
  RETURN_IF_ERROR(ReadVInt(byte_stream, &length));
  text->resize(length);
  RETURN_IF_ERROR(ReadBytes(byte_stream, length, text));
  return Status::OK;
}

std::string SerDeUtils::HexDump(const char* buf, int64_t length) {
  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < length; ++i) {
    ss << buf[i];
  }
  ss << std::dec;
  return ss.str();
}

bool SerDeUtils::IsNegativeVInt(int8_t byte) {
  return byte < -120 || (byte >= -112 && byte < 0);
}

int SerDeUtils::DecodeVIntSize(int8_t byte) {
  if (byte >= -112) {
    return 1;
  } else if (byte < -120) {
    return -119 - byte;
  }
  return -111 - byte;
}

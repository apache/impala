// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/serde-utils.h"

#include <vector>
#include <hdfs.h>

#include "common/status.h"

using namespace std;
using namespace impala;

Status SerDeUtils::ReadBoolean(hdfsFS fs, hdfsFile file, bool* boolean) {
  uint8_t byte;
  if (hdfsRead(fs, file, &byte, sizeof(byte)) != 1) {
    return Status("EOF encountered while reading Java BOOLEAN");
  }
  if (byte != 0) {
    *boolean = true;
  } else {
    *boolean = false;
  }
  return Status::OK;
}

Status SerDeUtils::ReadInt(hdfsFS fs, hdfsFile file, int32_t* integer) {
  uint8_t buf[sizeof(int)];
  if (hdfsRead(fs, file, &buf, sizeof(int32_t)) != sizeof(int32_t)) {
    return Status("EOF encountered while reading Java INT");
  }
  *integer =
      ((buf[0] & 0xff) << 24)
      | ((buf[1] & 0xff) << 16)
      | ((buf[2] & 0xff) << 8)
      |  (buf[3] & 0xff);
  return Status::OK;
}

Status SerDeUtils::ReadVLong(hdfsFS fs, hdfsFile file, int64_t* vlong) {
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
  
  if (hdfsRead(fs, file, &firstbyte, sizeof(firstbyte)) != 1) {
    return Status("EOF encountered while reading first byte of Writable VLONG");
  }
  
  int len = DecodeVIntSize(firstbyte);
  if (len == 1) {
    *vlong = (int64_t) firstbyte;
    return Status::OK;
  }
  --len;
  
  if (hdfsRead(fs, file, bytes, len) != len) {
    return Status("EOF encountered while reading Writable VLONG");
  }
  
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

Status SerDeUtils::ReadVInt(hdfsFS fs, hdfsFile file, int32_t* vint) {
  int64_t vlong;
  RETURN_IF_ERROR(ReadVLong(fs, file, &vlong));
  *vint = (int32_t) vlong;
  return Status::OK;
}

Status SerDeUtils::ReadBytes(hdfsFS fs, hdfsFile file, int32_t length,
                             std::vector<char>* buf) {
  buf->resize(length);
  if (length != hdfsRead(fs, file, reinterpret_cast<void*>(&(*buf)[0]), length)) {
    return Status("EOF encountered while reading bytes");
  }
  return Status::OK;
}

Status SerDeUtils::SkipBytes(hdfsFS fs, hdfsFile file, int32_t length) {
  tOffset off = hdfsTell(fs, file);
  if (off == -1) {
    return Status("Unable to determine current offset in file!");
  }
  if (hdfsSeek(fs, file, off + length) == -1) {
    return Status("Unable to seek to desired offset!");
  }
  return Status::OK;
}

Status SerDeUtils::ReadText(hdfsFS fs, hdfsFile file, std::vector<char>* text) {
  int32_t length;
  RETURN_IF_ERROR(ReadVInt(fs, file, &length));
  text->resize(length);
  RETURN_IF_ERROR(ReadBytes(fs, file, length, text));
  return Status::OK;
}

std::string SerDeUtils::HexDump(const char* buf, int32_t length) {
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

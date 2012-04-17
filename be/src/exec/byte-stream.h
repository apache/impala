// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_BYTE_STREAM_H_
#define IMPALA_EXEC_BYTE_STREAM_H_

#include <string>
#include <boost/cstdint.hpp>

namespace impala {

class Status;

// A simple wrapper around sources of byte data
class ByteStream {
 public:
  ByteStream() 
    : total_bytes_read_(0) {
  }
  virtual ~ByteStream() { }

  // Opens a resource from supplied location, ready for reading
  virtual Status Open(const std::string& location) = 0;

  // Relinquishes any obtained resources
  virtual Status Close() = 0;

  // Reads up to length bytes into buf, returning fewer if there is an error or EOF
  virtual Status Read(uint8_t* buf, int64_t req_length, int64_t* actual_length) = 0;

  // Positions the next read at offset bytes from the beginning of the
  // stream
  virtual Status Seek(int64_t offset) = 0;

  // Returns the position of the stream cursor
  virtual Status GetPosition(int64_t* position) = 0;

  // Returns if the stream is at EOF
  virtual Status Eof(bool* eof) = 0;

  // Returns the name of the resource backing this stream
  const std::string& GetLocation() { return location_; };

  // TODO: destructor to call Close

 protected:
  // Local copy of the resource location, for error-reporting
  std::string location_;

  int64_t total_bytes_read_;
};

}

#endif

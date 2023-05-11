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

#include "tuple-text-file-reader.h"

#include "common/names.h"

namespace impala {

TupleTextFileReader::TupleTextFileReader(const std::string& path) : path_(path) {}

Status TupleTextFileReader::Open() {
  if (!reader_.is_open()) {
    reader_.open(path_, std::ios::in | std::ios::binary);
    if (!reader_.is_open()) {
      return Status(TErrorCode::DISK_IO_ERROR,
          "open tuple text reader on " + path_ + " failed", GetStrErrMsg());
    }
    reader_.seekg(0, std::ios::end);
    file_size_ = reader_.tellg();
    reader_.seekg(0, std::ios::beg);
  }
  return Status();
}

void TupleTextFileReader::Close() {
  if (reader_.is_open()) {
    reader_.close();
  }
}

int TupleTextFileReader::GetFileSize() const {
  if (!reader_.is_open()) return TUPLE_TEXT_FILE_SIZE_ERROR;
  return file_size_;
}

void TupleTextFileReader::Rewind() {
  if (reader_.is_open()) {
    reader_.clear();
    reader_.seekg(0, std::ios::beg);
  }
}

Status TupleTextFileReader::GetNext(string* output, bool* eos) {
  DCHECK(output != nullptr);
  DCHECK(eos != nullptr);
  DCHECK(reader_.is_open());
  if (reader_.eof()) {
    *eos = true;
    return Status::OK();
  }
  getline(reader_, *output);
  if (!reader_.eof() && !reader_.good()) {
    return Status(TErrorCode::DISK_IO_ERROR, "tuple reader on " + path_, GetStrErrMsg());
  }
  *eos = false;
  return Status::OK();
}

} // namespace impala

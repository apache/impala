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

#include "bufferpool/buffer-pool.h"

#include <boost/bind.hpp>
#include <limits>
#include <sstream>

#include "bufferpool/reservation-tracker.h"
#include "common/names.h"
#include "gutil/strings/substitute.h"
#include "util/bit-util.h"
#include "util/uid-util.h"

using strings::Substitute;

namespace impala {

BufferPool::BufferPool(int64_t min_buffer_len, int64_t buffer_bytes_limit)
  : min_buffer_len_(min_buffer_len), buffer_bytes_limit_(buffer_bytes_limit) {
  DCHECK_GT(min_buffer_len, 0);
  DCHECK_EQ(min_buffer_len, BitUtil::RoundUpToPowerOfTwo(min_buffer_len));
}

BufferPool::~BufferPool() {}

Status BufferPool::RegisterClient(
    const string& name, ReservationTracker* reservation, Client* client) {
  DCHECK(!client->is_registered());
  DCHECK(reservation != NULL);
  client->reservation_ = reservation;
  client->name_ = name;
  return Status::OK();
}

void BufferPool::DeregisterClient(Client* client) {
  if (!client->is_registered()) return;
  client->reservation_->Close();
  client->name_.clear();
  client->reservation_ = NULL;
}

string BufferPool::Client::DebugString() const {
  return Substitute("<BufferPool::Client> $0 name: $1 reservation: $2", this, name_,
      reservation_->DebugString());
}

string BufferPool::DebugString() {
  stringstream ss;
  ss << "<BufferPool> " << this << " min_buffer_len: " << min_buffer_len_
     << "buffer_bytes_limit: " << buffer_bytes_limit_ << "\n";
  return ss.str();
}
}

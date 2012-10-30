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

#include "runtime/row-batch.h"

#include <stdint.h>  // for intptr_t

#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "gen-cpp/Data_types.h"

using namespace std;

namespace impala {

RowBatch::~RowBatch() {
  delete [] tuple_ptrs_;
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
}

void RowBatch::Serialize(TRowBatch* output_batch) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  output_batch->tuple_data.clear();

  output_batch->num_rows = num_rows_;
  row_desc_.ToThrift(&output_batch->row_tuples);
  output_batch->tuple_offsets.reserve(num_rows_ * num_tuples_per_row_);
  MemPool output_pool;

  // iterate through all tuples;
  // for a self-contained batch, convert Tuple* and string pointers into offsets into
  // our tuple_data_pool_;
  // otherwise, copy the tuple data, including strings, into output_pool (converting
  // string pointers into offset in the process)
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      if (row->GetTuple(j) == NULL) {
        // NULLs are encoded as -1
        output_batch->tuple_offsets.push_back(-1);
        continue;
      }

      Tuple* t;
      if (is_self_contained_) {
        t = row->GetTuple(j);
        output_batch->tuple_offsets.push_back(
            tuple_data_pool_->GetOffset(reinterpret_cast<uint8_t*>(t)));

        // convert string pointers to offsets
        vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
        for (; slot != (*desc)->string_slots().end(); ++slot) {
          DCHECK_EQ((*slot)->type(), TYPE_STRING);
          StringValue* string_val = t->GetStringSlot((*slot)->tuple_offset());
          string_val->ptr = reinterpret_cast<char*>(tuple_data_pool_->GetOffset(
                  reinterpret_cast<uint8_t*>(string_val->ptr)));
        }
      } else  {
        // record offset before creating copy
        output_batch->tuple_offsets.push_back(output_pool.GetCurrentOffset());
        t = row->GetTuple(j)->DeepCopy(**desc, &output_pool, /* convert_ptrs */ true);
      }
    }
  }

  if (is_self_contained_) {
    output_pool.AcquireData(tuple_data_pool_.get(), false);
    Reset();  // we passed on all data
  }

  vector<pair<uint8_t*, int> > chunk_info;
  output_pool.GetChunkInfo(&chunk_info);
  output_batch->tuple_data.reserve(chunk_info.size());
  for (int i = 0; i < chunk_info.size(); ++i) {
    // TODO: this creates yet another data copy, figure out how to avoid that
    // (Thrift should introduce something like StringPieces, that can hold
    // references to data/length pairs)
    string data(reinterpret_cast<char*>(chunk_info[i].first), chunk_info[i].second);
    output_batch->tuple_data.push_back(string());
    output_batch->tuple_data.back().swap(data);
    DCHECK(data.empty());
  }
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch)
  : has_in_flight_row_(false),
    is_self_contained_(true),
    num_rows_(input_batch.num_rows),
    capacity_(num_rows_),
    num_tuples_per_row_(input_batch.row_tuples.size()),
    row_desc_(row_desc),
    tuple_ptrs_(new Tuple*[num_rows_ * input_batch.row_tuples.size()]),
    tuple_data_pool_(new MemPool(input_batch.tuple_data)) {
  // convert input_batch.tuple_offsets into pointers
  int tuple_idx = 0;
  for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets.begin();
       offset != input_batch.tuple_offsets.end(); ++offset) {
    if (*offset == -1) {
      tuple_ptrs_[tuple_idx++] = NULL;
    } else {
      tuple_ptrs_[tuple_idx++] =
          reinterpret_cast<Tuple*>(tuple_data_pool_->GetDataPtr(*offset));
    }
  }

  // check whether we have string slots
  // TODO: do that during setup (part of RowDescriptor c'tor?)
  bool has_string_slots = false;
  const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
  for (int i = 0; i < tuple_descs.size(); ++i) {
    if (!tuple_descs[i]->string_slots().empty()) {
      has_string_slots = true;
      break;
    }
  }
  if (!has_string_slots) return;

  // convert string offsets contained in tuple data into pointers
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      if ((*desc)->string_slots().empty()) continue;
      Tuple* t = row->GetTuple(j);
      if (t == NULL) continue;

      vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
      for (; slot != (*desc)->string_slots().end(); ++slot) {
        DCHECK_EQ((*slot)->type(), TYPE_STRING);
        StringValue* string_val = t->GetStringSlot((*slot)->tuple_offset());
        string_val->ptr = reinterpret_cast<char*>(
            tuple_data_pool_->GetDataPtr(reinterpret_cast<intptr_t>(string_val->ptr)));
      }
    }
  }
}

int RowBatch::GetBatchSize(const TRowBatch& batch) {
  int result = 0;
  for (int i = 0; i < batch.tuple_data.size(); ++i) {
    result += batch.tuple_data[i].size();
  }
  return result;
}

void RowBatch::Swap(RowBatch* other) {
  DCHECK(row_desc_.Equals(other->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, other->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, other->tuple_ptrs_size_);

  // The destination row batch should be empty.  
  DCHECK(!has_in_flight_row_);
  DCHECK(!is_self_contained_);
  DCHECK(io_buffers_.empty());
  DCHECK_EQ(tuple_data_pool_->GetTotalChunkSizes(), 0);

  std::swap(has_in_flight_row_, other->has_in_flight_row_);
  std::swap(is_self_contained_, other->is_self_contained_);
  std::swap(num_rows_, other->num_rows_);
  std::swap(capacity_, other->capacity_);
  std::swap(tuple_ptrs_, other->tuple_ptrs_);
  std::swap(io_buffers_, other->io_buffers_);
  tuple_data_pool_.swap(other->tuple_data_pool_);
}

}

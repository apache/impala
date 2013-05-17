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
#include "util/compress.h"
#include "util/decompress.h"
#include "gen-cpp/Data_types.h"

DEFINE_bool(compress_rowbatches, true,
            "if true, compresses tuple data in Serialize");

using namespace std;

namespace impala {

RowBatch::~RowBatch() {
  delete [] tuple_ptrs_;
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
}

int RowBatch::Serialize(TRowBatch* output_batch) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  output_batch->is_compressed = false;

  output_batch->num_rows = num_rows_;
  row_desc_.ToThrift(&output_batch->row_tuples);
  output_batch->tuple_offsets.reserve(num_rows_ * num_tuples_per_row_);

  int size = TotalByteSize();
  output_batch->tuple_data.resize(size);

  // Copy tuple data, including strings, into output_batch (converting string
  // pointers into offsets in the process)
  int offset = 0; // current offset into output_batch->tuple_data
  char* tuple_data = const_cast<char*>(output_batch->tuple_data.c_str());
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
      // Record offset before creating copy (which increments offset and tuple_data)
      output_batch->tuple_offsets.push_back(offset);
      row->GetTuple(j)->DeepCopy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
      DCHECK_LE(offset, size);
    }
  }
  DCHECK_EQ(offset, size);

  if (FLAGS_compress_rowbatches && size > 0) {
    // Try compressing tuple_data to compression_scratch_, swap if compressed data is
    // smaller
    SnappyCompressor compressor;
    int compressed_size = compressor.MaxOutputLen(size);
    if (compression_scratch_.size() < compressed_size) {
      compression_scratch_.resize(compressed_size);
    }
    uint8_t* input = (uint8_t*)output_batch->tuple_data.c_str();
    uint8_t* compressed_output = (uint8_t*)compression_scratch_.c_str();
    compressor.ProcessBlock(size, input, &compressed_size, &compressed_output);
    if (LIKELY(compressed_size < size)) {
      compression_scratch_.resize(compressed_size);
      output_batch->tuple_data.swap(compression_scratch_);
      output_batch->is_compressed = true;
    }
    VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
  }

  // The size output_batch would be if we didn't compress tuple_data (will be equal to
  // actual batch size if tuple_data isn't compressed)
  return GetBatchSize(*output_batch) - output_batch->tuple_data.size() + size;
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch)
  : has_in_flight_row_(false),
    num_rows_(input_batch.num_rows),
    capacity_(num_rows_),
    num_tuples_per_row_(input_batch.row_tuples.size()),
    row_desc_(row_desc),
    tuple_ptrs_(new Tuple*[num_rows_ * input_batch.row_tuples.size()]),
    tuple_data_pool_(new MemPool(NULL)) {
  if (input_batch.is_compressed) {
    // Decompress tuple data into data pool
    uint8_t* compressed_data = (uint8_t*)input_batch.tuple_data.c_str();
    size_t compressed_size = input_batch.tuple_data.size();
    
    SnappyDecompressor decompressor;
    int uncompressed_size = decompressor.MaxOutputLen(compressed_size, compressed_data);
    DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
    uint8_t* data = tuple_data_pool_->Allocate(uncompressed_size);
    Status status = decompressor.ProcessBlock(compressed_size, compressed_data,
        &uncompressed_size, &data);
    DCHECK(status.ok()) << "RowBatch decompression failed.";
  } else {
    // Tuple data uncompressed, copy directly into data pool
    uint8_t* data = tuple_data_pool_->Allocate(input_batch.tuple_data.size());
    memcpy(data, input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
  }

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
  int result = batch.tuple_data.size();
  result += batch.row_tuples.size() * sizeof(TTupleId);
  result += batch.tuple_offsets.size() * sizeof(int32_t);
  return result;
}

void RowBatch::Swap(RowBatch* other) {
  DCHECK(row_desc_.Equals(other->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, other->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, other->tuple_ptrs_size_);

  // The destination row batch should be empty.  
  DCHECK(!has_in_flight_row_);
  DCHECK(io_buffers_.empty());
  DCHECK_EQ(tuple_data_pool_->GetTotalChunkSizes(), 0);

  std::swap(has_in_flight_row_, other->has_in_flight_row_);
  std::swap(num_rows_, other->num_rows_);
  std::swap(capacity_, other->capacity_);
  std::swap(tuple_ptrs_, other->tuple_ptrs_);
  std::swap(io_buffers_, other->io_buffers_);
  tuple_data_pool_.swap(other->tuple_data_pool_);
}

// TODO: consider computing size of batches as they are built up
int RowBatch::TotalByteSize() {
  int result = 0;
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      Tuple* tuple = row->GetTuple(j);
      if (tuple == NULL) continue;
      result += (*desc)->byte_size();
      vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
      for (; slot != (*desc)->string_slots().end(); ++slot) {
        DCHECK_EQ((*slot)->type(), TYPE_STRING);
        if (tuple->IsNull((*slot)->null_indicator_offset())) continue;
        StringValue* string_val = tuple->GetStringSlot((*slot)->tuple_offset());
        result += string_val->len;
      }
    }
  }
  return result;
}
}

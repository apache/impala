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

#include "exec/orc-column-readers.h"

#include <queue>

#include "runtime/collection-value-builder.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.inline.h"
#include "util/mem-util.h"
#include "common/names.h"

using namespace impala;

namespace impala {

string PrintNode(const orc::Type* node) {
  return Substitute("$0 column (ORC id=$1)", node->toString(), node->getColumnId());
}

bool OrcRowValidator::IsRowBatchValid() const {
  if (write_ids_ == nullptr || write_ids_->numElements == 0) return true;

  int64_t write_id = write_ids_->data[0];
  return valid_write_ids_.IsWriteIdValid(write_id);
}

OrcColumnReader* OrcColumnReader::Create(const orc::Type* node,
    const SlotDescriptor* slot_desc, HdfsOrcScanner* scanner) {
  DCHECK(node != nullptr);
  DCHECK(slot_desc != nullptr);
  OrcColumnReader* reader = nullptr;
  if (node->getKind() == orc::TypeKind::STRUCT) {
    reader = new OrcStructReader(node, slot_desc, scanner);
  } else if (node->getKind() == orc::TypeKind::LIST) {
    reader = new OrcListReader(node, slot_desc, scanner);
  } else if (node->getKind() == orc::TypeKind::MAP) {
    reader = new OrcMapReader(node, slot_desc, scanner);
  } else {
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        reader = new OrcBoolColumnReader(node, slot_desc, scanner);
        break;
      case TYPE_TINYINT:
        reader = new OrcIntColumnReader<int8_t>(node, slot_desc, scanner);
        break;
      case TYPE_SMALLINT:
        reader = new OrcIntColumnReader<int16_t>(node, slot_desc, scanner);
        break;
      case TYPE_INT:
        reader = new OrcIntColumnReader<int32_t>(node, slot_desc, scanner);
        break;
      case TYPE_BIGINT:
        reader = new OrcIntColumnReader<int64_t>(node, slot_desc, scanner);
        break;
      case TYPE_FLOAT:
        reader = new OrcDoubleColumnReader<float>(node, slot_desc, scanner);
        break;
      case TYPE_DOUBLE:
        reader = new OrcDoubleColumnReader<double>(node, slot_desc, scanner);
        break;
      case TYPE_TIMESTAMP:
        reader = new OrcTimestampReader(node, slot_desc, scanner);
        break;
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        reader = new OrcStringColumnReader(node, slot_desc, scanner);
        break;
      case TYPE_DECIMAL:
        if (node->getPrecision() == 0 || node->getPrecision() > 18) {
          // For decimals whose precision is larger than 18, its value can't fit into
          // an int64 (10^19 > 2^63). So we should use int128 for this case.
          reader = new OrcDecimal16ColumnReader(node, slot_desc, scanner);
        } else {
          switch (slot_desc->type().GetByteSize()) {
            case 4:
              reader = new OrcDecimalColumnReader<Decimal4Value>(
                  node, slot_desc, scanner);
              break;
            case 8:
              reader = new OrcDecimalColumnReader<Decimal8Value>(
                  node, slot_desc, scanner);
              break;
            case 16:
              reader = new OrcDecimalColumnReader<Decimal16Value>(
                  node, slot_desc, scanner);
              break;
            default:
              DCHECK(false) << "invalidate byte size for decimal type: "
                  << slot_desc->type().GetByteSize();
          }
        }
        break;
      case TYPE_DATE:
        reader = new OrcDateColumnReader(node, slot_desc, scanner);
        break;
      default:
        DCHECK(false) << slot_desc->type().DebugString();
    } // end of switch
  }
  return scanner->obj_pool_.Add(reader);
}

OrcComplexColumnReader* OrcComplexColumnReader::CreateTopLevelReader(
    const orc::Type* node, const TupleDescriptor* table_tuple_desc,
    HdfsOrcScanner* scanner) {
  OrcComplexColumnReader* reader = nullptr;
  if (node->getKind() == orc::TypeKind::STRUCT) {
    reader = new OrcStructReader(node, table_tuple_desc, scanner);
  } else if (node->getKind() == orc::TypeKind::LIST) {
    reader = new OrcListReader(node, table_tuple_desc, scanner);
  } else if (node->getKind() == orc::TypeKind::MAP) {
    reader = new OrcMapReader(node, table_tuple_desc, scanner);
  } else {
    DCHECK(false) << "Can't create top level reader for " << PrintNode(node);
  }
  return scanner->obj_pool_.Add(reader);
}

OrcColumnReader::OrcColumnReader(const orc::Type* orc_type,
    const SlotDescriptor* slot_desc, HdfsOrcScanner* scanner)
    : slot_desc_(slot_desc), scanner_(scanner) {
  orc_column_id_ = DCHECK_NOTNULL(orc_type)->getColumnId();
  if (slot_desc_ == nullptr) {
    orc::TypeKind type_kind = orc_type->getKind();
    DCHECK(type_kind == orc::TypeKind::LIST
        || type_kind == orc::TypeKind::MAP
        || type_kind == orc::TypeKind::STRUCT)
        << "Selected primitive types should have SlotDescriptors";
  }
  VLOG(3) << "Created reader for " << PrintNode(orc_type) << ": slot_desc_="
      << (slot_desc_? slot_desc_->DebugString() : "null");
}

Status OrcBoolColumnReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  int64_t val = batch_->data.data()[row_idx];
  *(reinterpret_cast<bool*>(GetSlot(tuple))) = (val != 0);
  return Status::OK();
}

Status OrcStringColumnReader::InitBlob(orc::DataBuffer<char>* blob, MemPool* pool) {
  // TODO: IMPALA-9310: Possible improvement is moving the buffer out from orc::DataBuffer
  // instead of copying and let Impala free the memory later.
  blob_ = reinterpret_cast<char*>(pool->TryAllocateUnaligned(blob->size()));
  if (UNLIKELY(blob_ == nullptr)) {
    string details = Substitute("Could not allocate string buffer of $0 bytes "
        "for ORC file '$1'.", blob->size(), scanner_->filename());
    return scanner_->scan_node_->mem_tracker()->MemLimitExceeded(
        scanner_->state_, details, blob->size());
  }
  memcpy(blob_, blob->data(), blob->size());
  return Status::OK();
}

Status OrcStringColumnReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  char* src_ptr;
  int src_len;

  if (batch_->isEncoded) {
    orc::EncodedStringVectorBatch* currentBatch =
        static_cast<orc::EncodedStringVectorBatch*>(batch_);

    orc::DataBuffer<int64_t>& offsets = currentBatch->dictionary->dictionaryOffset;
    int64_t index = currentBatch->index[row_idx];
    if (UNLIKELY(index < 0  || static_cast<uint64_t>(index) + 1 >= offsets.size())) {
      return Status(Substitute("Corrupt ORC file: $0. Index ($1) out of range [0, $2) in "
          "StringDictionary.", scanner_->filename(), index, offsets.size()));;
    }
    src_ptr = blob_ + offsets[index];
    src_len = offsets[index + 1] - offsets[index];
  } else {
    // The pointed data is now in blob_, a buffer handled by Impala.
    src_ptr = blob_ + (batch_->data[row_idx] - batch_->blob.data());
    src_len = batch_->length[row_idx];
  }
  int dst_len = slot_desc_->type().len;
  if (slot_desc_->type().type == TYPE_CHAR) {
    int unpadded_len = min(dst_len, static_cast<int>(src_len));
    char* dst_char = reinterpret_cast<char*>(GetSlot(tuple));
    memcpy(dst_char, src_ptr, unpadded_len);
    StringValue::PadWithSpaces(dst_char, dst_len, unpadded_len);
    return Status::OK();
  }
  StringValue* dst = reinterpret_cast<StringValue*>(GetSlot(tuple));
  if (slot_desc_->type().type == TYPE_VARCHAR && src_len > dst_len) {
    dst->len = dst_len;
  } else {
    dst->len = src_len;
  }
  dst->ptr = src_ptr;
  return Status::OK();
}

Status OrcTimestampReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  int64_t secs = batch_->data.data()[row_idx];
  int64_t nanos = batch_->nanoseconds.data()[row_idx];
  auto slot = reinterpret_cast<TimestampValue*>(GetSlot(tuple));
  *slot = TimestampValue::FromUnixTimeNanos(secs, nanos, UTCPTR);
  if (UNLIKELY(!slot->HasDate())) {
    SetNullSlot(tuple);
    TErrorCode::type errorCode = TErrorCode::ORC_TIMESTAMP_OUT_OF_RANGE;
    ErrorMsg msg(errorCode, scanner_->filename(), orc_column_id_);
    return scanner_->state_->LogOrReturnError(msg);
  }
  return Status::OK();
}

Status OrcDateColumnReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  DateValue dv(batch_->data.data()[row_idx]);
  if (UNLIKELY(!dv.IsValid())) {
    SetNullSlot(tuple);
    ErrorMsg msg(TErrorCode::ORC_DATE_OUT_OF_RANGE, scanner_->filename(), orc_column_id_);
    return scanner_->state_->LogOrReturnError(msg);
  }
  DateValue* slot = reinterpret_cast<DateValue*>(GetSlot(tuple));
  *slot = dv;
  return Status::OK();
}

Status OrcDecimal16ColumnReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  orc::Int128 orc_val = batch_->values.data()[row_idx];

  DCHECK_EQ(slot_desc_->type().GetByteSize(), 16);
  __int128_t val = orc_val.getHighBits();
  val <<= 64;
  val |= orc_val.getLowBits();
  // Use memcpy to avoid gcc generating unaligned instructions like movaps
  // for int128_t. They will raise SegmentFault when addresses are not
  // aligned to 16 bytes.
  memcpy(GetSlot(tuple), &val, sizeof(__int128_t));
  return Status::OK();
}

OrcComplexColumnReader::OrcComplexColumnReader(const orc::Type* node,
    const TupleDescriptor* table_tuple_desc, HdfsOrcScanner* scanner)
    : OrcBatchedReader(node, nullptr, scanner) {
  uint64_t node_col_id = node->getColumnId();
  uint64_t tuple_desc_col_id = GetColId(table_tuple_desc);
  if (node_col_id == tuple_desc_col_id) tuple_desc_ = table_tuple_desc;
  materialize_tuple_ = (tuple_desc_ != nullptr);
  VLOG(3) << "Created top level ComplexColumnReader for " << PrintNode(node)
      << ": tuple_desc_=" << (tuple_desc_ ? tuple_desc_->DebugString() : "null");
}

bool OrcStructReader::EndOfBatch() {
  DCHECK(slot_desc_ == nullptr
      && (tuple_desc_ == nullptr || tuple_desc_ == scanner_->scan_node_->tuple_desc()))
      << "Should be top level reader when calling EndOfBatch()";
  DCHECK(vbatch_ == nullptr || row_idx_ <= NumElements());
  return vbatch_ == nullptr || row_idx_ == NumElements();
}

inline uint64_t OrcComplexColumnReader::GetTargetColId(
    const SlotDescriptor* slot_desc) const {
  return slot_desc->type().IsCollectionType() ?
         GetColId(slot_desc->collection_item_descriptor()):
         GetColId(slot_desc);
}

/**
 * Returns true if 'candidate_col_id' is under 'node' in the type hierarchy.
 */
inline bool IsDescendant(const orc::Type& node, uint64_t candidate_col_id) {
  uint64_t node_col_id = node.getColumnId();
  uint64_t node_max_col_id = node.getMaximumColumnId();
  return node_col_id <= candidate_col_id && candidate_col_id <= node_max_col_id;
}

/**
 * 'parent' is a complex type that might have multiple children, i.e. STRUCT or MAP.
 * 'descendant_col_id' refers to an ORC type somewhere under 'parent' in the type
 * hieararchy. This method selects the direct child of 'parent' that is also the ancestor
 * of 'descendant_col_id'. Return the result in '*child' and its index in '*field'.
 * Returns false for not found.
 */
bool FindChild(const orc::Type& parent, uint64_t descendant_col_id,
    const orc::Type** child, int* field) {
  DCHECK(parent.getKind() == orc::TypeKind::STRUCT ||
         parent.getKind() == orc::TypeKind::MAP);
  int size = parent.getSubtypeCount();
  for (int c = 0; c < size; ++c) {
    const orc::Type* node = parent.getSubtype(c);
    if (node && IsDescendant(*node, descendant_col_id)) {
      *child = node;
      *field = c;
      return true;
    }
  }
  return false;
}

void OrcStructReader::CreateChildForSlot(const orc::Type* curr_node,
    const SlotDescriptor* slot_desc) {
  // 'slot_desc' matches a descendant of 'curr_node' which may not be a direct child.
  // Find a child node that lays in the path from 'curr_node' to the descendant.
  // Create a child reader and pass down 'slot_desc'.
  const orc::Type* child_node;
  int field;
  if (!FindChild(*curr_node, GetTargetColId(slot_desc), &child_node, &field)) {
    DCHECK(false) << PrintNode(curr_node) << " has no children selected for "
        << slot_desc->DebugString();
  }
  OrcColumnReader* child = OrcColumnReader::Create(child_node, slot_desc, scanner_);
  children_.push_back(child);
  children_fields_.push_back(field);
}

OrcStructReader::OrcStructReader(const orc::Type* node,
    const TupleDescriptor* table_tuple_desc, HdfsOrcScanner* scanner)
    : OrcComplexColumnReader(node, table_tuple_desc, scanner) {
  bool needs_row_validation = table_tuple_desc == scanner_->scan_node_->tuple_desc() &&
                              node->getColumnId() == 0 &&
                              scanner_->row_batches_need_validation_;
  if (materialize_tuple_) {
    for (SlotDescriptor* child_slot : tuple_desc_->slots()) {
      // Skip partition columns and missed columns
      if (scanner->IsPartitionKeySlot(child_slot)
          || scanner->IsMissingField(child_slot)) {
        continue;
      }
      CreateChildForSlot(node, child_slot);
    }
  } else {
    // No tuples should be materialized by this reader, because 'table_tuple_desc'
    // matches to a descendant of 'node'. Those tuples should be materialized by the
    // corresponding descendant reader. So 'node' should have exactly one selected
    // subtype: the child in the path to the target descendant.
    DCHECK_EQ(node->getSubtypeCount(), needs_row_validation ? 2 : 1);
    int child_index = needs_row_validation ? 1 : 0;
    OrcComplexColumnReader* child = OrcComplexColumnReader::CreateTopLevelReader(
        node->getSubtype(child_index), table_tuple_desc, scanner);
    children_.push_back(child);
    children_fields_.push_back(child_index);
  }
  if (needs_row_validation) {
    row_validator_.reset(new OrcRowValidator(scanner_->valid_write_ids_));
    for (int i = 0; i < node->getSubtypeCount(); ++i) {
      if (node->getSubtype(i)->getColumnId() == CURRENT_TRANSCACTION_TYPE_ID) {
        current_write_id_field_index_ = i;
        break;
      }
    }
  }
}

OrcStructReader::OrcStructReader(const orc::Type* node,
    const SlotDescriptor* slot_desc, HdfsOrcScanner* scanner)
    : OrcComplexColumnReader(node, slot_desc, scanner) {
  // 'slot_desc' won't map to a STRUCT column. It only matches a descendant column.
  // If the descendant column is missing in the file, skip creating the child reader.
  if (scanner->IsMissingField(slot_desc)) return;
  CreateChildForSlot(node, slot_desc);
  VLOG(3) << "Created StructReader for " << PrintNode(node) << ": slot_desc_="
      << slot_desc->DebugString();
}

Status OrcStructReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (!MaterializeTuple()) {
    DCHECK_EQ(1, children_.size());
    OrcColumnReader* child = children_[0];
    return child->ReadValue(row_idx, tuple, pool);
  }
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
    for (OrcColumnReader* child : children_) child->SetNullSlot(tuple);
    return Status::OK();
  }
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->ReadValue(row_idx, tuple, pool));
  }
  return Status::OK();
}

Status OrcStructReader::TopLevelReadValueBatch(ScratchTupleBatch* scratch_batch,
    MemPool* pool) {
  // Validate row batch if needed.
  if (row_validator_) DCHECK(scanner_->row_batches_need_validation_);
  if (row_validator_ && !row_validator_->IsRowBatchValid()) {
    row_idx_ = NumElements();
    return Status::OK();
  }
  // Saving the initial value of num_tuples because each child->ReadValueBatch() will
  // update it.
  int scratch_batch_idx = scratch_batch->num_tuples;
  int item_count = -1;
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(
        child->ReadValueBatch(row_idx_, scratch_batch, pool, scratch_batch_idx));
    // Check if each column reader reads the same amount of values.
    if (item_count == -1) item_count = scratch_batch->num_tuples;
    if (item_count != scratch_batch->num_tuples) {
      return Status(Substitute("Corrupt ORC file '$0':  Expected number of items in "
          "each column: $1 Actual number in col '$2': $3", scanner_->filename(),
          item_count, orc_column_id_, scratch_batch->num_tuples));
    }
  }
  int num_rows_read = scratch_batch->num_tuples - scratch_batch_idx;
  if (children_.empty()) {
    // We allow empty 'children_' for original files, because we might select the
    // synthetic 'rowid' field which is not present in original files.
    // We also allow empty 'children_' when we need to validate row batches of a zero slot
    // scan. In that case 'children_' is empty and only 'row_validator_' owns an ORC
    // vector batch (the write id batch).
    bool valid_empty_children = scanner_->acid_original_file_ ||
         (scanner_->row_batches_need_validation_ &&
          scanner_->scan_node_->IsZeroSlotTableScan());
    if (!valid_empty_children) {
      return Status(Substitute("Parse error in possibly corrupt ORC file: '$0'",
          scanner_->filename()));
    }
    DCHECK_EQ(0, num_rows_read);
    num_rows_read = std::min(scratch_batch->capacity - scratch_batch->num_tuples,
                             NumElements() - row_idx_);
    scratch_batch->num_tuples += num_rows_read;
  }
  if (scanner_->acid_synthetic_rowid_ != nullptr) {
    FillSyntheticRowId(scratch_batch, scratch_batch_idx, num_rows_read);
  }
  row_idx_ += num_rows_read;
  return Status::OK();
}

void OrcStructReader::FillSyntheticRowId(ScratchTupleBatch* scratch_batch,
    int scratch_batch_idx, int num_rows) {
    DCHECK(scanner_->acid_synthetic_rowid_ != nullptr);
    int tuple_size = OrcColumnReader::scanner_->tuple_byte_size();
    uint8_t* first_tuple = scratch_batch->tuple_mem + scratch_batch_idx * tuple_size;
    int64_t* first_slot = reinterpret_cast<Tuple*>(first_tuple)->GetBigIntSlot(
        scanner_->acid_synthetic_rowid_->tuple_offset());
    StrideWriter<int64_t> out{first_slot, tuple_size};
    for (int i = 0; i < num_rows; ++i) {
      *out.Advance() = file_row_idx_++;
    }
}

Status OrcStructReader::ReadValueBatch(int row_idx, ScratchTupleBatch* scratch_batch,
    MemPool* pool, int scratch_batch_idx) {
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(
        child->ReadValueBatch(row_idx, scratch_batch, pool, scratch_batch_idx));
  }
  return Status::OK();
}

Status OrcStructReader::UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) {
  RETURN_IF_ERROR(OrcComplexColumnReader::UpdateInputBatch(orc_batch));
  batch_ = static_cast<orc::StructVectorBatch*>(orc_batch);
  // In debug mode, we use dynamic_cast<> to double-check the downcast is legal
  DCHECK(batch_ == dynamic_cast<orc::StructVectorBatch*>(orc_batch));
  if (batch_ == nullptr || batch_->numElements == 0) {
    row_idx_ = 0;
    for (OrcColumnReader* child : children_) {
      RETURN_IF_ERROR(child->UpdateInputBatch(nullptr));
    }
    return Status::OK();
  }
  row_idx_ = 0;
  int size = children_.size();
  for (int c = 0; c < size; ++c) {
    RETURN_IF_ERROR(children_[c]->UpdateInputBatch(batch_->fields[children_fields_[c]]));
  }
  if (row_validator_) {
    orc::ColumnVectorBatch* write_id_batch =
        batch_->fields[current_write_id_field_index_];
    DCHECK_EQ(static_cast<orc::LongVectorBatch*>(write_id_batch),
              dynamic_cast<orc::LongVectorBatch*>(write_id_batch));
    row_validator_->UpdateTransactionBatch(
        static_cast<orc::LongVectorBatch*>(write_id_batch));
  }
  return Status::OK();
}

OrcCollectionReader::OrcCollectionReader(const orc::Type* node,
    const SlotDescriptor* slot_desc, HdfsOrcScanner* scanner)
    : OrcComplexColumnReader(node, slot_desc, scanner) {
  if (slot_desc->type().IsCollectionType() &&
      node->getColumnId() == GetTargetColId(slot_desc)) {
    // This is a collection SlotDescriptor whose item TupleDescriptor matches
    // 'node'. We should materialize the slot (creating a CollectionValue) and its
    // collection tuples (see more in HdfsOrcScanner::AssembleCollection).
    tuple_desc_ = slot_desc->collection_item_descriptor();
    materialize_tuple_ = true;
  }
}

Status OrcCollectionReader::AssembleCollection(int row_idx, Tuple* tuple, MemPool* pool) {
  if (IsNull(DCHECK_NOTNULL(vbatch_), row_idx)) {
    SetNullSlot(tuple);
    return Status::OK();
  }
  auto coll_slot = reinterpret_cast<CollectionValue*>(GetSlot(tuple));
  *coll_slot = CollectionValue();
  const TupleDescriptor* tuple_desc = slot_desc_->collection_item_descriptor();
  CollectionValueBuilder builder(coll_slot, *tuple_desc, pool, scanner_->state_);
  return scanner_->AssembleCollection(*this, row_idx, &builder);
}

int OrcListReader::NumElements() const {
  if (DirectReader()) return batch_ != nullptr ? batch_->numElements : 0;
  if (children_.empty()) {
    return batch_ != nullptr ? batch_->offsets[batch_->numElements] : 0;
  }
  return children_[0]->NumElements();
}

Status OrcListReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (DirectReader()) return AssembleCollection(row_idx, tuple, pool);

  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->ReadValue(row_idx, tuple, pool));
  }
  if (pos_slot_desc_ != nullptr) {
    RETURN_IF_ERROR(SetPositionSlot(row_idx, tuple));
  }
  return Status::OK();
}

Status OrcMapReader::ReadValue(int row_idx, Tuple* tuple, MemPool* pool) {
  if (DirectReader()) return AssembleCollection(row_idx, tuple, pool);

  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->ReadValue(row_idx, tuple, pool));
  }
  return Status::OK();
}

Status OrcListReader::SetPositionSlot(int row_idx, Tuple* tuple) {
  DCHECK(pos_slot_desc_ != nullptr);

  int64_t pos = -1;
  DCHECK_LT(list_idx_, batch_->numElements);
  if (list_idx_ == batch_->numElements - 1 ||
      (batch_->offsets[list_idx_] <= row_idx && row_idx < batch_->offsets[list_idx_+1])) {
    // We are somewhere in the current list.
    pos = row_idx - batch_->offsets[list_idx_];
  } else if (row_idx == batch_->offsets[list_idx_+1]) {
    // Let's move to the next list.
    pos = 0;
    list_idx_ += 1;
  }
  else if (row_idx > batch_->offsets[list_idx_+1]) {
    // We lagged behind. Let's find our list.
    for (int i = list_idx_; i < batch_->numElements; ++i) {
      if (row_idx < batch_->offsets[i+1]) {
        pos = row_idx - batch_->offsets[i];
        list_idx_ = i;
        break;
      }
    }
  }
  if (pos < 0) {
      // Oops, something went wrong. It can be caused by a corrupt file, so let's raise
      // an error.
      return Status(Substitute(
          "ORC list indexes and elements are inconsistent in file $0",
          scanner_->filename()));
  }
  int64_t* slot_val_ptr = reinterpret_cast<int64_t*>(tuple->GetSlot(
      pos_slot_desc_->tuple_offset()));
  *slot_val_ptr = pos;
  return Status::OK();
}

void OrcListReader::CreateChildForSlot(const orc::Type* node,
    const SlotDescriptor* slot_desc) {
  uint64_t slot_col_id = GetTargetColId(slot_desc);
  DCHECK(IsDescendant(*node, slot_col_id));
  // We have a position slot descriptor if it refers to this LIST ORC type, but it isn't
  // a collection slot.
  bool is_pos_slot = slot_col_id == node->getColumnId() &&
                     slot_desc->collection_item_descriptor() == nullptr;
  if (is_pos_slot) {
    DCHECK(pos_slot_desc_ == nullptr) << "Should have unique pos slot";
    pos_slot_desc_ = slot_desc;
  } else {
    OrcColumnReader* child = OrcColumnReader::Create(node->getSubtype(0), slot_desc,
        scanner_);
    children_.push_back(child);
  }
}

OrcListReader::OrcListReader(const orc::Type* node,
    const TupleDescriptor* table_tuple_desc, HdfsOrcScanner* scanner)
    : OrcCollectionReader(node, table_tuple_desc, scanner) {
  if (materialize_tuple_) {
    DCHECK(tuple_desc_ != nullptr);
    for (SlotDescriptor* child_slot : tuple_desc_->slots()) {
      CreateChildForSlot(node, child_slot);
    }
  } else {
    OrcComplexColumnReader* child = OrcComplexColumnReader::CreateTopLevelReader(
        node->getSubtype(0), table_tuple_desc, scanner);
    children_.push_back(child);
  }
}

OrcListReader::OrcListReader(const orc::Type* node, const SlotDescriptor* slot_desc,
    HdfsOrcScanner* scanner) : OrcCollectionReader(node, slot_desc, scanner) {
  if (materialize_tuple_) {
    DCHECK(tuple_desc_ != nullptr);
    for (SlotDescriptor* child_slot : tuple_desc_->slots()) {
      CreateChildForSlot(node, child_slot);
    }
  } else {
    // 'slot_desc' matches a descendant instead. Create a child reader for the child node
    // laying in the path to the descendant.
    CreateChildForSlot(node, slot_desc);
  }
  VLOG(3) << "Created ListReader for " << PrintNode(node) << ": tuple_desc_="
      << (tuple_desc_ != nullptr ? tuple_desc_->DebugString() : "null");
}

Status OrcListReader::UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) {
  RETURN_IF_ERROR(OrcComplexColumnReader::UpdateInputBatch(orc_batch));
  batch_ = static_cast<orc::ListVectorBatch*>(orc_batch);
  // In debug mode, we use dynamic_cast<> to double-check the downcast is legal
  DCHECK(batch_ == dynamic_cast<orc::ListVectorBatch*>(orc_batch));
  orc::ColumnVectorBatch* item_batch = batch_ ? batch_->elements.get() : nullptr;
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->UpdateInputBatch(item_batch));
  }
  list_idx_ = 0;
  return Status::OK();
}

int OrcListReader::GetNumChildValues(int row_idx) const {
  if (IsNull(DCHECK_NOTNULL(batch_), row_idx)) return 0;
  DCHECK_GT(batch_->offsets.size(), row_idx + 1);
  return batch_->offsets[row_idx + 1] - batch_->offsets[row_idx];
}

int OrcListReader::GetChildBatchOffset(int row_idx) const {
  return batch_->offsets[row_idx];
}

Status OrcListReader::ReadChildrenValue(int row_idx, int tuple_idx, Tuple* tuple,
    MemPool* pool) const {
  DCHECK_LT(row_idx, batch_->numElements);
  int offset = batch_->offsets[row_idx];
  if (pos_slot_desc_) {
    int64_t* slot_val_ptr = reinterpret_cast<int64_t*>(
        tuple->GetSlot(pos_slot_desc_->tuple_offset()));
    *slot_val_ptr = tuple_idx;
  }
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->ReadValue(offset + tuple_idx, tuple, pool));
  }
  return Status::OK();
}

void OrcMapReader::CreateChildForSlot(const orc::Type* node,
    const SlotDescriptor* slot_desc) {
  const orc::Type* child_type;
  int field;
  if (!FindChild(*node, GetTargetColId(slot_desc), &child_type, &field)) {
    DCHECK(false) << PrintNode(node) << " has no children selected for "
        << slot_desc->DebugString();
  }
  // Map type only has two children.
  DCHECK_LE(field, 1);
  OrcColumnReader* child = OrcColumnReader::Create(child_type, slot_desc, scanner_);
  children_.push_back(child);
  if (field == 0) {
    key_readers_.push_back(child);
  } else {
    value_readers_.push_back(child);
  }
}

OrcMapReader::OrcMapReader(const orc::Type* node,
    const TupleDescriptor* table_tuple_desc, HdfsOrcScanner* scanner)
    : OrcCollectionReader(node, table_tuple_desc, scanner) {
  if (materialize_tuple_) {
    DCHECK(tuple_desc_ != nullptr);
    for (SlotDescriptor* child_slot : tuple_desc_->slots()) {
      CreateChildForSlot(node, child_slot);
    }
  } else {
    const orc::Type* child_type;
    int field;
    if (!FindChild(*node, GetColId(table_tuple_desc),
        &child_type, &field)) {
      DCHECK(false) << PrintNode(node) << " has no children selected for "
          << table_tuple_desc->DebugString();
    }
    // Map type only has two children.
    DCHECK_LE(field, 1);
    OrcComplexColumnReader* child = OrcComplexColumnReader::CreateTopLevelReader(
        child_type, table_tuple_desc, scanner);
    children_.push_back(child);
    if (field == 0) {
      key_readers_.push_back(child);
    } else {
      value_readers_.push_back(child);
    }
  }
  VLOG(3) << "Created MapReader for " << PrintNode(node) << ": tuple_desc_="
      << (tuple_desc_ != nullptr ? tuple_desc_->DebugString() : "null");
}

OrcMapReader::OrcMapReader(const orc::Type* node, const SlotDescriptor* slot_desc,
    HdfsOrcScanner* scanner) : OrcCollectionReader(node, slot_desc, scanner) {
  if (materialize_tuple_) {
    DCHECK(tuple_desc_ != nullptr);
    for (SlotDescriptor* child_slot : tuple_desc_->slots()) {
      CreateChildForSlot(node, child_slot);
    }
  } else {
    CreateChildForSlot(node, slot_desc);
  }
}

Status OrcMapReader::UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) {
  RETURN_IF_ERROR(OrcComplexColumnReader::UpdateInputBatch(orc_batch));
  batch_ = static_cast<orc::MapVectorBatch*>(orc_batch);
  // In debug mode, we use dynamic_cast<> to double-check the downcast is legal
  DCHECK(batch_ == dynamic_cast<orc::MapVectorBatch*>(orc_batch));
  orc::ColumnVectorBatch* key_batch = batch_ ? batch_->keys.get() : nullptr;
  orc::ColumnVectorBatch* value_batch = batch_ ? batch_->elements.get() : nullptr;
  for (OrcColumnReader* child : key_readers_) {
    RETURN_IF_ERROR(child->UpdateInputBatch(key_batch));
  }
  for (OrcColumnReader* child : value_readers_) {
    RETURN_IF_ERROR(child->UpdateInputBatch(value_batch));
  }
  return Status::OK();
}

int OrcMapReader::GetNumChildValues(int row_idx) const {
  if (IsNull(batch_, row_idx)) return 0;
  DCHECK_GT(batch_->offsets.size(), row_idx + 1);
  return batch_->offsets[row_idx + 1] - batch_->offsets[row_idx];
}

int OrcMapReader::GetChildBatchOffset(int row_idx) const {
  return batch_->offsets[row_idx];
}

Status OrcMapReader::ReadChildrenValue(int row_idx, int tuple_idx, Tuple* tuple,
    MemPool* pool) const {
  DCHECK_LT(row_idx, batch_->numElements);
  int offset = batch_->offsets[row_idx];
  for (OrcColumnReader* child : children_) {
    RETURN_IF_ERROR(child->ReadValue(offset + tuple_idx, tuple, pool));
  }
  return Status::OK();
}

int OrcMapReader::NumElements() const {
  if (DirectReader()) return batch_ != nullptr ? batch_->numElements : 0;
  if (children_.empty()) {
    return batch_ != nullptr ? batch_->offsets[batch_->numElements] : 0;
  }
  return children_[0]->NumElements();
}

} // namespace impala

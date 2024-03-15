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

#include "exec/file-metadata-utils.h"

#include "exec/hdfs-scan-node-base.h"
#include "exec/scanner-context.h"
#include "exec/text-converter.inline.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "util/flat_buffer.h"
#include "util/string-parser.h"

#include "common/names.h"

namespace impala {

void FileMetadataUtils::SetFile(RuntimeState* state, const HdfsFileDesc* file_desc) {
  DCHECK(state != nullptr);
  DCHECK(file_desc != nullptr);
  state_ = state;
  file_desc_ = file_desc;
}

Tuple* FileMetadataUtils::CreateTemplateTuple(int64_t partition_id, MemPool* mem_pool,
    std::map<const SlotId, const SlotDescriptor*>* slot_descs_written) {
  DCHECK(file_desc_ != nullptr);
  // Initialize the template tuple, it is copied from the template tuple map in the
  // HdfsScanNodeBase.
  Tuple* template_tuple = scan_node_->GetTemplateTupleForPartitionId(partition_id);
  if (template_tuple != nullptr) {
    template_tuple = template_tuple->DeepCopy(*scan_node_->tuple_desc(), mem_pool);
  }
  if (UNLIKELY(!scan_node_->virtual_column_slots().empty())) {
    AddFileLevelVirtualColumns(mem_pool, template_tuple);
  }
  if (scan_node_->hdfs_table()->IsIcebergTable()) {
    AddIcebergColumns(mem_pool, &template_tuple, slot_descs_written);
  }
  return template_tuple;
}

void FileMetadataUtils::AddFileLevelVirtualColumns(MemPool* mem_pool,
    Tuple* template_tuple) {
  if (template_tuple == nullptr) return;
  for (int i = 0; i < scan_node_->virtual_column_slots().size(); ++i) {
    const SlotDescriptor* slot_desc = scan_node_->virtual_column_slots()[i];
    if (slot_desc->virtual_column_type() == TVirtualColumnType::INPUT_FILE_NAME) {
      StringValue* slot = template_tuple->GetStringSlot(slot_desc->tuple_offset());
      const char* filename = file_desc_->filename.c_str();
      int len = strlen(filename);
      char* filename_copy = reinterpret_cast<char*>(mem_pool->Allocate(len));
      Ubsan::MemCpy(filename_copy, filename, len);
    slot->Assign(filename_copy, len);
      template_tuple->SetNotNull(slot_desc->null_indicator_offset());
    } else if (slot_desc->virtual_column_type() ==
        TVirtualColumnType::ICEBERG_DATA_SEQUENCE_NUMBER) {
      using namespace org::apache::impala::fb;
      const FbIcebergMetadata* ice_metadata =
          file_desc_->file_metadata->iceberg_metadata();
      DCHECK(ice_metadata != nullptr);

      int64_t data_seq_num = ice_metadata->data_sequence_number();
      if (data_seq_num > -1) {
        int64_t* slot = template_tuple->GetBigIntSlot(slot_desc->tuple_offset());
        *slot = data_seq_num;
        template_tuple->SetNotNull(slot_desc->null_indicator_offset());
      } else {
        template_tuple->SetNull(slot_desc->null_indicator_offset());
      }
    }
  }
}

void FileMetadataUtils::AddIcebergColumns(MemPool* mem_pool, Tuple** template_tuple,
    std::map<const SlotId, const SlotDescriptor*>* slot_descs_written) {
  using namespace org::apache::impala::fb;
  TextConverter text_converter(/* escape_char = */ '\\',
      scan_node_->hdfs_table()->null_partition_key_value(),
      /* check_null = */ true, /* strict_mode = */ true);
  const FbFileMetadata* file_metadata = file_desc_->file_metadata;
  const FbIcebergMetadata* ice_metadata = file_metadata->iceberg_metadata();
  auto transforms = ice_metadata->partition_keys();

  const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();
  if (*template_tuple == nullptr) {
    *template_tuple = Tuple::Create(tuple_desc->byte_size(), mem_pool);
  }
  for (const SlotDescriptor* slot_desc : scan_node_->tuple_desc()->slots()) {
    if (slot_desc->IsVirtual()) {
      AddVirtualIcebergColumn(mem_pool, *template_tuple, *ice_metadata, slot_desc);
      continue;
    }
    if (transforms == nullptr) continue;
    const SchemaPath& path = slot_desc->col_path();
    if (path.size() != 1) continue;
    const ColumnDescriptor& col_desc =
        scan_node_->hdfs_table()->col_descs()[path.front()];
    int field_id = col_desc.field_id();
    for (int i = 0; i < transforms->size(); ++i) {
      auto transform = transforms->Get(i);
      if (transform->transform_type() !=
          FbIcebergTransformType::FbIcebergTransformType_IDENTITY) {
        continue;
      }
      if (field_id != transform->source_id()) continue;
      if (!text_converter.WriteSlot(slot_desc, *template_tuple,
                                    (const char*)transform->transform_value()->data(),
                                    transform->transform_value()->size(),
                                    true, false,
                                    mem_pool)) {
        ErrorMsg error_msg(TErrorCode::GENERAL,
            Substitute("Could not parse partition value for "
                "column '$0' in file '$1'. Partition string is '$2' "
                "NULL Partition key value is '$3'",
                col_desc.name(), file_desc_->filename,
                transform->transform_value()->data(),
                scan_node_->hdfs_table()->null_partition_key_value()));
        // Dates are stored as INTs in the partition data in Iceberg, so let's try
        // to parse them as INTs.
        if (col_desc.type().type == PrimitiveType::TYPE_DATE) {
          int32_t* slot = (*template_tuple)->GetIntSlot(slot_desc->tuple_offset());
          StringParser::ParseResult parse_result;
          *slot = StringParser::StringToInt<int32_t>(
              (const char*)transform->transform_value()->data(),
              transform->transform_value()->size(),
              &parse_result);
          if (parse_result == StringParser::ParseResult::PARSE_SUCCESS) {
            (*template_tuple)->SetNotNull(slot_desc->null_indicator_offset());
            slot_descs_written->insert({slot_desc->id(), slot_desc});
          } else {
            state_->LogError(error_msg);
          }
        } else {
          state_->LogError(error_msg);
        }
      } else {
        slot_descs_written->insert({slot_desc->id(), slot_desc});
      }
    }
  }
}

void FileMetadataUtils::AddVirtualIcebergColumn(MemPool* mem_pool, Tuple* template_tuple,
      const org::apache::impala::fb::FbIcebergMetadata& ice_metadata,
      const SlotDescriptor* slot_desc) {
  DCHECK(slot_desc->IsVirtual());
  if (slot_desc->virtual_column_type() == TVirtualColumnType::PARTITION_SPEC_ID) {
    int32_t* slot = template_tuple->GetIntSlot(slot_desc->tuple_offset());
    *slot = ice_metadata.spec_id();
  } else if (slot_desc->virtual_column_type() ==
             TVirtualColumnType::ICEBERG_PARTITION_SERIALIZED) {
    auto transforms = ice_metadata.partition_keys();
    if (transforms == nullptr) return;
    string partitions;
    for (int i = 0; i < transforms->size(); ++i) {
      using namespace org::apache::impala::fb;
      auto transform = transforms->Get(i);
      if (transform->transform_type() ==
            FbIcebergTransformType::FbIcebergTransformType_VOID) {
        continue;
      }
      stringstream part_ss;
      Base64Encode((const char*)transform->transform_value()->data(),
                   transform->transform_value()->size(), &part_ss);
      string part_value(part_ss.str());
      if (partitions.empty()) {
        partitions = part_value;
      } else {
        partitions += '.' + part_value;
      }
    }
    StringValue* slot = template_tuple->GetStringSlot(slot_desc->tuple_offset());
    int len = partitions.length();
    char* partition_serialized_copy = reinterpret_cast<char*>(mem_pool->Allocate(len));
    Ubsan::MemCpy(partition_serialized_copy, partitions.c_str(), len);
    slot->Assign(partition_serialized_copy, len);
    template_tuple->SetNotNull(slot_desc->null_indicator_offset());
  }
}

bool FileMetadataUtils::IsValuePartitionCol(const SlotDescriptor* slot_desc) {
  DCHECK(file_desc_ != nullptr);
  if (slot_desc->col_pos() < scan_node_->num_partition_keys() &&
      !slot_desc->IsVirtual()) {
    return true;
  }

  if (!scan_node_->hdfs_table()->IsIcebergTable()) return false;

  using namespace org::apache::impala::fb;

  const SchemaPath& path = slot_desc->col_path();
  if (path.size() != 1) return false;

  int field_id = scan_node_->hdfs_table()->col_descs()[path.front()].field_id();
  const FbFileMetadata* file_metadata = file_desc_->file_metadata;
  const FbIcebergMetadata* ice_metadata = file_metadata->iceberg_metadata();
  auto transforms = ice_metadata->partition_keys();
  if (transforms == nullptr) return false;
  for (int i = 0; i < transforms->size(); ++i) {
    auto transform = transforms->Get(i);
    if (transform->source_id() == field_id &&
        transform->transform_type() ==
            FbIcebergTransformType::FbIcebergTransformType_IDENTITY) {
      return true;
    }
  }
  return false;
}

bool FileMetadataUtils::NeedDataInFile(const SlotDescriptor* slot_desc) {
  if (IsValuePartitionCol(slot_desc)) return false;
  if (slot_desc->IsVirtual()) return false;
  return true;
}

} // namespace impala

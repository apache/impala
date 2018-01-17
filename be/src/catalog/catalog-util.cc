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


#include <boost/algorithm/string.hpp>
#include <sstream>

#include "catalog/catalog-util.h"
#include "exec/read-write-util.h"
#include "util/compress.h"
#include "util/jni-util.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::algorithm::to_upper_copy;

namespace impala {

jclass JniCatalogCacheUpdateIterator::pair_cl;
jmethodID JniCatalogCacheUpdateIterator::pair_ctor;
jclass JniCatalogCacheUpdateIterator::boolean_cl;
jmethodID JniCatalogCacheUpdateIterator::boolean_ctor;

Status JniCatalogCacheUpdateIterator::InitJNI() {
  JNIEnv* env = getJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "Lorg/apache/impala/common/Pair;", &pair_cl));
  pair_ctor = env->GetMethodID(pair_cl, "<init>",
      "(Ljava/lang/Object;Ljava/lang/Object;)V");
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "Ljava/lang/Boolean;", &boolean_cl));
  boolean_ctor = env->GetMethodID(boolean_cl, "<init>", "(Z)V");
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniCatalogCacheUpdateIterator::createPair(JNIEnv* env, bool deleted,
    const uint8_t* buffer, long size, jobject* out) {
  jobject deleted_obj = env->NewObject(boolean_cl, boolean_ctor,
      static_cast<jboolean>(deleted));
  RETURN_ERROR_IF_EXC(env);
  jobject byte_buffer = env->NewDirectByteBuffer(const_cast<uint8_t*>(buffer), size);
  RETURN_ERROR_IF_EXC(env);
  *out = env->NewObject(pair_cl, pair_ctor, deleted_obj, byte_buffer);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

jobject TopicItemSpanIterator::next(JNIEnv* env) {
  while (begin_ != end_) {
    jobject result;
    Status s;
    const TTopicItem* current = begin_++;
    if (decompress_) {
      s = DecompressCatalogObject(
          reinterpret_cast<const uint8_t*>(current->value.data()),
          static_cast<uint32_t>(current->value.size()), &decompressed_buffer_);
      if (!s.ok()) {
        LOG(ERROR) << "Error decompressing catalog object: " << s.GetDetail();
        continue;
      }
      s = createPair(env, current->deleted,
          reinterpret_cast<const uint8_t*>(decompressed_buffer_.data()),
          static_cast<long>(decompressed_buffer_.size()), &result);
    } else {
      s = createPair(env, current->deleted,
          reinterpret_cast<const uint8_t*>(current->value.data()),
          static_cast<long>(current->value.size()), &result);
    }
    if (s.ok()) return result;
    LOG(ERROR) << "Error creating return value: " << s.GetDetail();
  }
  return nullptr;
}

jobject CatalogUpdateResultIterator::next(JNIEnv* env) {
  const vector<TCatalogObject>& removed = result_.removed_catalog_objects;
  const vector<TCatalogObject>& updated = result_.updated_catalog_objects;
  while (pos_ != removed.size() + updated.size()) {
    bool deleted;
    const TCatalogObject* current_obj;
    if (pos_ < removed.size()) {
      current_obj = &removed[pos_];
      deleted = true;
    } else {
      current_obj = &updated[pos_ - removed.size()];
      deleted = false;
    }
    ++pos_;
    uint8_t* buf;
    uint32_t buf_size;
    Status s = serializer_.Serialize(current_obj, &buf_size, &buf);
    if (!s.ok()) {
      LOG(ERROR) << "Error serializing catalog object: " << s.GetDetail();
      continue;
    }
    jobject result = nullptr;
    s = createPair(env, deleted, buf, buf_size, &result);
    if (s.ok()) return result;
    LOG(ERROR) << "Error creating jobject." << s.GetDetail();
  }
  return nullptr;
}

TCatalogObjectType::type TCatalogObjectTypeFromName(const string& name) {
  const string& upper = to_upper_copy(name);
  if (upper == "DATABASE") {
    return TCatalogObjectType::DATABASE;
  } else if (upper == "TABLE") {
    return TCatalogObjectType::TABLE;
  } else if (upper == "VIEW") {
    return TCatalogObjectType::VIEW;
  } else if (upper == "FUNCTION") {
    return TCatalogObjectType::FUNCTION;
  } else if (upper == "CATALOG") {
    return TCatalogObjectType::CATALOG;
  } else if (upper == "DATA_SOURCE") {
    return TCatalogObjectType::DATA_SOURCE;
  } else if (upper == "HDFS_CACHE_POOL") {
    return TCatalogObjectType::HDFS_CACHE_POOL;
  } else if (upper == "ROLE") {
    return TCatalogObjectType::ROLE;
  } else if (upper == "PRIVILEGE") {
    return TCatalogObjectType::PRIVILEGE;
  }
  return TCatalogObjectType::UNKNOWN;
}

Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const string& object_name, TCatalogObject* catalog_object) {
  switch (object_type) {
    case TCatalogObjectType::DATABASE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_db(TDatabase());
      catalog_object->db.__set_db_name(object_name);
      break;
    case TCatalogObjectType::TABLE:
    case TCatalogObjectType::VIEW: {
      catalog_object->__set_type(object_type);
      catalog_object->__set_table(TTable());
      // Parse what should be a fully qualified table name
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid table name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->table.__set_db_name(object_name.substr(0, pos));
      catalog_object->table.__set_tbl_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::FUNCTION: {
      // The key looks like: <db>.fn(<args>). We need to parse out the
      // db, fn and signature.
      catalog_object->__set_type(object_type);
      catalog_object->__set_fn(TFunction());
      int dot = object_name.find(".");
      int paren = object_name.find("(");
      if (dot == string::npos || dot >= object_name.size() - 1 ||
          paren == string::npos || paren >= object_name.size() - 1 ||
          paren <= dot) {
        stringstream error_msg;
        error_msg << "Invalid function name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->fn.name.__set_db_name(object_name.substr(0, dot));
      catalog_object->fn.name.__set_function_name(
          object_name.substr(dot + 1, paren - dot - 1));
      catalog_object->fn.__set_signature(object_name.substr(dot + 1));
      break;
    }
    case TCatalogObjectType::DATA_SOURCE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_data_source(TDataSource());
      catalog_object->data_source.__set_name(object_name);
      break;
    case TCatalogObjectType::HDFS_CACHE_POOL:
      catalog_object->__set_type(object_type);
      catalog_object->__set_cache_pool(THdfsCachePool());
      catalog_object->cache_pool.__set_pool_name(object_name);
      break;
    case TCatalogObjectType::ROLE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_role(TRole());
      catalog_object->role.__set_role_name(object_name);
      break;
    case TCatalogObjectType::PRIVILEGE: {
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid privilege name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->__set_type(object_type);
      catalog_object->__set_privilege(TPrivilege());
      catalog_object->privilege.__set_role_id(atoi(object_name.substr(0, pos).c_str()));
      catalog_object->privilege.__set_privilege_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::CATALOG:
    case TCatalogObjectType::UNKNOWN:
    default:
      stringstream error_msg;
      error_msg << "Unexpected object type: " << object_type;
      return Status(error_msg.str());
  }
  return Status::OK();
}

Status CompressCatalogObject(const uint8_t* src, uint32_t size, string* dst) {
  scoped_ptr<Codec> compressor;
  RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, THdfsCompression::LZ4,
      &compressor));
  int64_t compressed_data_len = compressor->MaxOutputLen(size);
  int64_t output_buffer_len = compressed_data_len + sizeof(uint32_t);
  dst->resize(static_cast<size_t>(output_buffer_len));
  uint8_t* output_buffer_ptr = reinterpret_cast<uint8_t*>(&((*dst)[0]));
  ReadWriteUtil::PutInt(output_buffer_ptr, size);
  output_buffer_ptr += sizeof(uint32_t);
  RETURN_IF_ERROR(compressor->ProcessBlock(true, size, src, &compressed_data_len,
      &output_buffer_ptr));
  dst->resize(compressed_data_len + sizeof(uint32_t));
  return Status::OK();
}

Status DecompressCatalogObject(const uint8_t* src, uint32_t size, string* dst) {
  scoped_ptr<Codec> decompressor;
  RETURN_IF_ERROR(Codec::CreateDecompressor(nullptr, false, THdfsCompression::LZ4,
      &decompressor));
  int64_t decompressed_len = ReadWriteUtil::GetInt<uint32_t>(src);
  dst->resize(static_cast<size_t>(decompressed_len));
  uint8_t* decompressed_data_ptr = reinterpret_cast<uint8_t*>(&((*dst)[0]));
  RETURN_IF_ERROR(decompressor->ProcessBlock(true, size - sizeof(uint32_t),
      src + sizeof(uint32_t), &decompressed_len, &decompressed_data_ptr));
  return Status::OK();
}

}

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
#include <boost/algorithm/string_regex.hpp>
#include <sstream>

#include "catalog/catalog-util.h"
#include "exec/read-write-util.h"
#include "util/compress.h"
#include "util/jni-util.h"
#include "util/debug-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::algorithm::to_upper_copy;

namespace impala {

jclass JniCatalogCacheUpdateIterator::pair_cl;
jmethodID JniCatalogCacheUpdateIterator::pair_ctor;
jclass JniCatalogCacheUpdateIterator::boolean_cl;
jmethodID JniCatalogCacheUpdateIterator::boolean_ctor;

/// Populates a TPrivilegeLevel::type based on the given object name string.
Status TPrivilegeLevelFromObjectName(const std::string& object_name,
    TPrivilegeLevel::type* privilege_level);

Status JniCatalogCacheUpdateIterator::InitJNI() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "org/apache/impala/common/Pair", &pair_cl));
  pair_ctor = env->GetMethodID(pair_cl, "<init>",
      "(Ljava/lang/Object;Ljava/lang/Object;)V");
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(
      JniUtil::GetGlobalClassRef(env, "java/lang/Boolean", &boolean_cl));
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
    jobject result = nullptr;
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
    Status s = serializer_.SerializeToBuffer(current_obj, &buf_size, &buf);
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
  } else if (upper == "PRINCIPAL") {
    return TCatalogObjectType::PRINCIPAL;
  } else if (upper == "PRIVILEGE") {
    return TCatalogObjectType::PRIVILEGE;
  }
  // TODO(IMPALA-9935): support HDFS_PARTITION
  return TCatalogObjectType::UNKNOWN;
}

Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const string& object_name, TCatalogObject* catalog_object) {
  // See Catalog::toCatalogObjectKey in Catalog.java for more information on the
  // catalog object key format.
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
      int pos = object_name.find('.');
      if (pos == string::npos || pos >= object_name.size() - 1) {
        stringstream error_msg;
        error_msg << "Invalid table name: " << object_name;
        return Status(error_msg.str());
      }
      catalog_object->table.__set_db_name(object_name.substr(0, pos));
      catalog_object->table.__set_tbl_name(object_name.substr(pos + 1));
      break;
    }
    // TODO(IMPALA-9935): support HDFS_PARTITION
    case TCatalogObjectType::FUNCTION: {
      // The key looks like: <db>.fn(<args>). We need to parse out the
      // db, fn and signature.
      catalog_object->__set_type(object_type);
      catalog_object->__set_fn(TFunction());
      int dot = object_name.find('.');
      int paren = object_name.find('(');
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
    case TCatalogObjectType::PRINCIPAL: {
      // The format is <principal name>.<principal type>
      vector<string> split;
      boost::split(split, object_name, [](char c) { return c == '.'; });
      if (split.size() != 2) {
        stringstream error_msg;
        error_msg << "Invalid principal name: " << object_name;
        return Status(error_msg.str());
      }
      string principal_name = split[0];
      string principal_type = split[1];
      catalog_object->__set_type(object_type);
      catalog_object->__set_principal(TPrincipal());
      catalog_object->principal.__set_principal_name(principal_name);
      if (principal_type == "ROLE") {
        catalog_object->principal.__set_principal_type(TPrincipalType::ROLE);
      } else if (principal_type == "USER") {
        catalog_object->principal.__set_principal_type(TPrincipalType::USER);
      } else {
        stringstream error_msg;
        error_msg << "Invalid principal type: " << principal_type;
        return Status(error_msg.str());
      }
      break;
    }
    case TCatalogObjectType::PRIVILEGE: {
      // The format is <privilege name>.<principal ID>.<principal type>
      vector<string> split;
      boost::split(split, object_name, [](char c){ return c == '.'; });
      if (split.size() != 3) {
        stringstream error_msg;
        error_msg << "Invalid privilege name: " << object_name;
        return Status(error_msg.str());
      }
      string privilege_name = split[0];
      string principal_id = split[1];
      string principal_type = split[2];
      catalog_object->__set_type(object_type);
      TPrivilege privilege;
      Status status = TPrivilegeFromObjectName(privilege_name, &privilege);
      if (!status.ok()) return status;
      catalog_object->__set_privilege(privilege);
      StringParser::ParseResult result;
      int32_t pid = StringParser::StringToInt<int32_t>(principal_id.c_str(),
          principal_id.length(), &result);
      if (result != StringParser::PARSE_SUCCESS) {
        stringstream error_msg;
        error_msg << "Invalid principal ID: " << principal_id;
        return Status(error_msg.str());
      }
      catalog_object->privilege.__set_principal_id(pid);
      if (principal_type == "ROLE") {
        catalog_object->privilege.__set_principal_type(TPrincipalType::ROLE);
      } else if (principal_type == "USER") {
        catalog_object->privilege.__set_principal_type(TPrincipalType::USER);
      } else {
        stringstream error_msg;
        error_msg << "Invalid principal type: " << principal_type;
        return Status(error_msg.str());
      }
      break;
    }
    case TCatalogObjectType::AUTHZ_CACHE_INVALIDATION: {
      catalog_object->__set_type(object_type);
      catalog_object->__set_authz_cache_invalidation(TAuthzCacheInvalidation());
      catalog_object->authz_cache_invalidation.__set_marker_name(object_name);
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

Status TPrivilegeFromObjectName(const string& object_name, TPrivilege* privilege) {
  DCHECK(privilege != nullptr);
  // Format:
  // server=val->action=val->grantoption=[true|false]
  // server=val->uri=val->action=val->grantoption=[true|false]
  // server=val->db=val->action=val->grantoption=[true|false]
  // server=val->db=val->table=val->action=val->grantoption=[true|false]
  // server=val->db=val->table=val->column=val->action=val->grantoption=[true|false]
  vector<string> split;
  boost::algorithm::split_regex(split, object_name, boost::regex("->"));
  for (const auto& s: split) {
    vector<string> key_value;
    boost::split(key_value, s, [](char c){ return c == '='; });
    if (key_value.size() != 2) {
      stringstream error_msg;
      error_msg << "Invalid field name/value format: " << s;
      return Status(error_msg.str());
    }

    if (key_value[0] == "server") {
      privilege->__set_server_name(key_value[1]);
      privilege->__set_scope(TPrivilegeScope::SERVER);
    } else if (key_value[0] == "uri") {
      privilege->__set_uri(key_value[1]);
      privilege->__set_scope(TPrivilegeScope::URI);
    } else if (key_value[0] == "db") {
      privilege->__set_db_name(key_value[1]);
      privilege->__set_scope(TPrivilegeScope::DATABASE);
    } else if (key_value[0] == "table") {
      privilege->__set_table_name(key_value[1]);
      privilege->__set_scope(TPrivilegeScope::TABLE);
    } else if (key_value[0] == "column") {
      privilege->__set_column_name(key_value[1]);
      privilege->__set_scope(TPrivilegeScope::COLUMN);
    } else if (key_value[0] == "action") {
      TPrivilegeLevel::type privilege_level;
      Status status = TPrivilegeLevelFromObjectName(key_value[1], &privilege_level);
      if (!status.ok()) return status;
      privilege->__set_privilege_level(privilege_level);
    } else if (key_value[0] == "grantoption") {
      if (key_value[1] == "true") {
        privilege->__set_has_grant_opt(true);
      } else if (key_value[1] == "false") {
        privilege->__set_has_grant_opt(false);
      } else {
        stringstream error_msg;
        error_msg << "Invalid grant option: " << key_value[1];
        return Status(error_msg.str());
      }
    } else {
      stringstream error_msg;
      error_msg << "Invalid privilege field name: " << key_value[0];
      return Status(error_msg.str());
    }
  }
  return Status::OK();
}

Status CompressCatalogObject(const uint8_t* src, uint32_t size, string* dst) {
  scoped_ptr<Codec> compressor;
  Codec::CodecInfo codec_info(THdfsCompression::LZ4);
  RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, codec_info, &compressor));
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

Status TPrivilegeLevelFromObjectName(const std::string& object_name,
    TPrivilegeLevel::type* privilege_level) {
  DCHECK(privilege_level != nullptr);
  if (object_name == "all") {
    *privilege_level = TPrivilegeLevel::ALL;
  } else if (object_name == "insert") {
    *privilege_level = TPrivilegeLevel::INSERT;
  } else if (object_name == "select") {
    *privilege_level = TPrivilegeLevel::SELECT;
  } else if (object_name == "refresh") {
    *privilege_level = TPrivilegeLevel::REFRESH;
  } else if (object_name == "create") {
    *privilege_level = TPrivilegeLevel::CREATE;
  } else if (object_name == "alter") {
    *privilege_level = TPrivilegeLevel::ALTER;
  } else if (object_name == "drop") {
    *privilege_level = TPrivilegeLevel::DROP;
  } else if (object_name == "owner") {
    *privilege_level = TPrivilegeLevel::OWNER;
  } else {
    stringstream error_msg;
    error_msg << "Invalid privilege level: " << object_name;
    return Status(error_msg.str());
  }
  return Status::OK();
}

}

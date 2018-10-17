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


#ifndef IMPALA_CATALOG_CATALOG_UTIL_H
#define IMPALA_CATALOG_CATALOG_UTIL_H

#include <jni.h>
#include <gen-cpp/StatestoreService_types.h>
#include <gen-cpp/CatalogService_types.h>
#include <rpc/thrift-util.h>

#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"

namespace impala {

/// A helper class used to pass catalog object updates to the FE. With this iterator, the
/// catalog objects are decompressed and transferred to the FE one by one without having
/// to keep the entire uncompressed catalog objects in memory.
class JniCatalogCacheUpdateIterator {
 public:
  /// Initialize JNI classes and method IDs. Currently it is only initilized in impalad.
  static Status InitJNI();

  /// Return the next catalog object from a catalog update. The return type is
  /// Pair<Boolean, ByteBuffer>. The Boolean value is true if the update is a delete
  /// operation. The ByteBuffer is the serialized TCatalogObject. null is returned at the
  /// end of the update set. The return value is invalided on the next call.
  /// If the deserialization or decompression of an object is unsuccessful, the object
  /// will be skipped and the next valid object is returned.
  virtual jobject next(JNIEnv* env) = 0;

  virtual ~JniCatalogCacheUpdateIterator() = default;

 protected:
  /// A helper function used to create the return value of next().
  Status createPair(JNIEnv* env, bool deleted, const uint8_t* buffer, long size,
      jobject* out);

 private:
  static jclass pair_cl;
  static jmethodID pair_ctor;
  static jclass boolean_cl;
  static jmethodID boolean_ctor;
};

/// Pass catalog objects in CatalogUpdateCallback().
class TopicItemSpanIterator : public JniCatalogCacheUpdateIterator {
 public:
  TopicItemSpanIterator(const vector<TTopicItem>& items, bool decompress) :
      begin_(items.data()), end_(items.data() + items.size()),
      decompress_(decompress) {}

  jobject next(JNIEnv* env) override;

 private:
  const TTopicItem* begin_;
  const TTopicItem* end_;
  bool decompress_;
  std::string decompressed_buffer_;
};

/// Pass catalog objects in ProcessCatalogUpdateResult().
class CatalogUpdateResultIterator : public JniCatalogCacheUpdateIterator {
 public:
  explicit CatalogUpdateResultIterator(const TCatalogUpdateResult& catalog_update_result)
      : result_(catalog_update_result), pos_(0), serializer_(false) {}

  jobject next(JNIEnv* env) override;

 private:
  const TCatalogUpdateResult& result_;
  int pos_;
  ThriftSerializer serializer_;
};

/// Converts a string to the matching TCatalogObjectType enum type. Returns
/// TCatalogObjectType::UNKNOWN if no match was found.
TCatalogObjectType::type TCatalogObjectTypeFromName(const std::string& name);

/// Populates a TCatalogObject based on the given object type (TABLE, DATABASE, etc) and
/// object name string.
Status TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const std::string& object_name, TCatalogObject* catalog_object);

/// Populates a TPrivilege based on the given object name string.
Status TPrivilegeFromObjectName(const std::string& object_name, TPrivilege* privilege);

/// Compresses a serialized catalog object using LZ4 and stores it back in 'dst'. Stores
/// the size of the uncompressed catalog object in the first sizeof(uint32_t) bytes of
/// 'dst'. The compression fails if the uncompressed data size exceeds 0x7E000000 bytes.
Status CompressCatalogObject(const uint8_t* src, uint32_t size, std::string* dst)
    WARN_UNUSED_RESULT;

/// Decompress an LZ4-compressed catalog object. The decompressed object is stored in
/// 'dst'. The first sizeof(uint32_t) bytes of 'src' store the size of the uncompressed
/// catalog object.
Status DecompressCatalogObject(const uint8_t* src, uint32_t size, std::string* dst)
    WARN_UNUSED_RESULT;
}

#endif

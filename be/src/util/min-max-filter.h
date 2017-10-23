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

#ifndef IMPALA_UTIL_MIN_MAX_FILTER_H
#define IMPALA_UTIL_MIN_MAX_FILTER_H

#include "gen-cpp/ImpalaInternalService_types.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/string-buffer.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

class MemPool;
class ObjectPool;

/// A MinMaxFilter tracks the min and max currently seen values in a data set for use in
/// runtime filters.
///
/// Filters are constructed using MinMaxFilter::Create() which returns a MinMaxFilter of
/// the appropriate type. Values can then be added using Insert(), and the min and max can
/// be retrieved using GetMin()/GetMax().
///
/// MinMaxFilters ignore NULL values, and so are only appropriate to use as a runtime
/// filter if the join predicate is '=' and not 'is not distinct from'.
class MinMaxFilter {
 public:
  virtual ~MinMaxFilter() {}

  /// Returns the min/max values in the tuple slot representation. It is not valid to call
  /// these functions if AlwaysFalse() returns true.
  virtual void* GetMin() = 0;
  virtual void* GetMax() = 0;

  /// Returns the min/max values in the out paramsters 'out_min'/'out_max', converted to
  /// fit into 'type', eg. if the calculated max value is greater than the max value for
  /// 'type', the returned max is the max for 'type'. Returns false if the entire range
  /// from the calculated min to max is outside the range for 'type'. May only be called
  /// for integer-typed filters.
  virtual bool GetCastIntMinMax(
      const ColumnType& type, int64_t* out_min, int64_t* out_max);

  virtual PrimitiveType type() = 0;

  /// Add a new value, updating the current min/max.
  virtual void Insert(void* val) = 0;

  /// If true, this filter allows all rows to pass.
  virtual bool AlwaysTrue() const = 0;

  /// If true, this filter doesn't allow any rows to pass.
  virtual bool AlwaysFalse() const = 0;

  /// Materialize filter values by copying any values stored by filters into memory owned
  /// by the filter. Filters may assume that the memory for Insert()-ed values stays valid
  /// until this is called.
  virtual void MaterializeValues() {}

  /// Convert this filter to a thrift representation.
  virtual void ToThrift(TMinMaxFilter* thrift) const = 0;

  virtual std::string DebugString() const = 0;

  /// Returns a new MinMaxFilter with the given type, allocated from 'pool'.
  static MinMaxFilter* Create(ColumnType type, ObjectPool* pool, MemPool* mem_pool);

  /// Returns a new MinMaxFilter created from the thrift representation, allocated from
  /// 'pool'.
  static MinMaxFilter* Create(
      const TMinMaxFilter& thrift, ColumnType type, ObjectPool* pool, MemPool* mem_pool);

  /// Computes the logical OR of 'in' with 'out' and stores the result in 'out'.
  static void Or(const TMinMaxFilter& in, TMinMaxFilter* out);

  /// Copies the contents of 'in' into 'out'.
  static void Copy(const TMinMaxFilter& in, TMinMaxFilter* out);

  /// Returns the LLVM_CLASS_NAME for the given type.
  static std::string GetLlvmClassName(PrimitiveType type);

  /// Returns the IRFunction::Type for Insert() for the given type.
  static IRFunction::Type GetInsertIRFunctionType(PrimitiveType type);
};

#define NUMERIC_MIN_MAX_FILTER(NAME, TYPE)                                    \
  class NAME##MinMaxFilter : public MinMaxFilter {                            \
   public:                                                                    \
    NAME##MinMaxFilter() {                                                    \
      min_ = std::numeric_limits<TYPE>::max();                                \
      max_ = std::numeric_limits<TYPE>::lowest();                             \
    }                                                                         \
    NAME##MinMaxFilter(const TMinMaxFilter& thrift);                          \
    virtual ~NAME##MinMaxFilter() {}                                          \
    virtual void* GetMin() override { return &min_; }                         \
    virtual void* GetMax() override { return &max_; }                         \
    virtual bool GetCastIntMinMax(                                            \
        const ColumnType& type, int64_t* out_min, int64_t* out_max) override; \
    virtual PrimitiveType type() override;                                    \
    virtual void Insert(void* val) override;                                  \
    virtual bool AlwaysTrue() const override { return false; }                \
    virtual bool AlwaysFalse() const override {                               \
      return min_ == std::numeric_limits<TYPE>::max()                         \
          && max_ == std::numeric_limits<TYPE>::lowest();                     \
    }                                                                         \
    virtual void ToThrift(TMinMaxFilter* thrift) const override;              \
    virtual std::string DebugString() const override;                         \
    static void Or(const TMinMaxFilter& in, TMinMaxFilter* out);              \
    static void Copy(const TMinMaxFilter& in, TMinMaxFilter* out);            \
    static const char* LLVM_CLASS_NAME;                                       \
                                                                              \
   private:                                                                   \
    TYPE min_;                                                                \
    TYPE max_;                                                                \
  };

NUMERIC_MIN_MAX_FILTER(Bool, bool);
NUMERIC_MIN_MAX_FILTER(TinyInt, int8_t);
NUMERIC_MIN_MAX_FILTER(SmallInt, int16_t);
NUMERIC_MIN_MAX_FILTER(Int, int32_t);
NUMERIC_MIN_MAX_FILTER(BigInt, int64_t);
NUMERIC_MIN_MAX_FILTER(Float, float);
NUMERIC_MIN_MAX_FILTER(Double, double);

class StringMinMaxFilter : public MinMaxFilter {
 public:
  StringMinMaxFilter(MemPool* mem_pool)
    : min_buffer_(mem_pool),
      max_buffer_(mem_pool),
      always_false_(true),
      always_true_(false) {}
  StringMinMaxFilter(const TMinMaxFilter& thrift, MemPool* mem_pool);
  virtual ~StringMinMaxFilter() {}

  virtual void* GetMin() override { return &min_; }
  virtual void* GetMax() override { return &max_; }
  virtual PrimitiveType type() override;

  virtual void Insert(void* val) override;
  virtual bool AlwaysTrue() const override { return always_true_; }
  virtual bool AlwaysFalse() const override { return always_false_; }

  /// Copies the values pointed to by 'min_'/'max_' into 'min_buffer_'/'max_buffer_',
  /// truncating them if necessary.
  virtual void MaterializeValues() override;

  virtual void ToThrift(TMinMaxFilter* thrift) const override;
  virtual std::string DebugString() const override;

  static void Or(const TMinMaxFilter& in, TMinMaxFilter* out);
  static void Copy(const TMinMaxFilter& in, TMinMaxFilter* out);

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  /// Copies the contents of 'value' into 'buffer', up to 'len', and reassignes 'value' to
  /// point to 'buffer'. If an oom is hit, disables the filter by setting 'always_true_'
  /// to true.
  void CopyToBuffer(StringBuffer* buffer, StringValue* value, int64_t len);

  /// Sets 'always_true_' to true and clears the values of 'min_', 'max_', 'min_buffer_',
  /// and 'max_buffer_'.
  void SetAlwaysTrue();

  /// The maximum length of string to store in 'min_str_' or 'max_str_'. Strings inserted
  /// into this filter that are longer than this will be truncated.
  static const int MAX_BOUND_LENGTH;

  /// The min/max values. After a call to MaterializeValues() these will point to
  /// 'min_buffer_'/'max_buffer_'.
  StringValue min_;
  StringValue max_;

  /// Local buffers to copy min/max data into. If Insert() was called and 'min_'/'max_'
  /// was updated, these will be empty until MaterializeValues() is called.
  StringBuffer min_buffer_;
  StringBuffer max_buffer_;

  /// True if no rows have been inserted.
  bool always_false_;
  bool always_true_;
};

class TimestampMinMaxFilter : public MinMaxFilter {
 public:
  TimestampMinMaxFilter() { always_false_ = true; }
  TimestampMinMaxFilter(const TMinMaxFilter& thrift);
  virtual ~TimestampMinMaxFilter() {}

  virtual void* GetMin() override { return &min_; }
  virtual void* GetMax() override { return &max_; }
  virtual PrimitiveType type() override;

  virtual void Insert(void* val) override;
  virtual bool AlwaysTrue() const override { return false; }
  virtual bool AlwaysFalse() const override { return always_false_; }
  virtual void ToThrift(TMinMaxFilter* thrift) const override;
  virtual std::string DebugString() const override;

  static void Or(const TMinMaxFilter& in, TMinMaxFilter* out);
  static void Copy(const TMinMaxFilter& in, TMinMaxFilter* out);

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  TimestampValue min_;
  TimestampValue max_;

  /// True if no rows have been inserted.
  bool always_false_;
};
}

#endif

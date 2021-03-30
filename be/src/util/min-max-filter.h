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
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

class MemTacker;
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
  MinMaxFilter() : always_true_(false) {}
  virtual ~MinMaxFilter() {}
  virtual void Close() {}

  /// Returns the min/max values in the tuple slot representation. It is not valid to call
  /// these functions if AlwaysFalse() returns true.
  virtual const void* GetMin() const = 0;
  virtual const void* GetMax() const = 0;

  /// Returns the min/max values in the out paramsters 'out_min'/'out_max', converted to
  /// fit into 'type', eg. if the calculated max value is greater than the max value for
  /// 'type', the returned max is the max for 'type'. Returns false if the entire range
  /// from the calculated min to max is outside the range for 'type'. May only be called
  /// for integer-typed filters.
  virtual bool GetCastIntMinMax(
      const ColumnType& type, int64_t* out_min, int64_t* out_max);

  /// Determine whether two ranges: [data_min, data_max] and [filter_min, filter_max]
  /// overlap. Return true if the two overlaps and false otherwise. Both data_min and
  /// data_max are of type 'type'.
  virtual bool EvalOverlap(const ColumnType& type, void* data_min, void* data_max) = 0;

  virtual PrimitiveType type() = 0;

  /// Add a new value, updating the current min/max.
  virtual void Insert(const void* val) = 0;

  /// If true, this filter doesn't allow any rows to pass.
  virtual bool AlwaysFalse() const = 0;

  /// Materialize filter values by copying any values stored by filters into memory owned
  /// by the filter. Filters may assume that the memory for Insert()-ed values stays valid
  /// until this is called.
  virtual void MaterializeValues() {}

  /// Convert this filter to a protobuf representation.
  virtual void ToProtobuf(MinMaxFilterPB* protobuf) const = 0;

  virtual std::string DebugString() const = 0;

  /// Returns a new MinMaxFilter with the given type, allocated from 'mem_tracker'.
  static MinMaxFilter* Create(ColumnType type, ObjectPool* pool, MemTracker* mem_tracker);

  /// Returns a new MinMaxFilter created from the protobuf representation, allocated from
  /// 'mem_tracker'.
  static MinMaxFilter* Create(const MinMaxFilterPB& protobuf, ColumnType type,
      ObjectPool* pool, MemTracker* mem_tracker);

  /// Updates this filter with the logical OR of this filter and 'other'.
  void Or(const MinMaxFilter& other);

  /// Computes the logical OR of 'in' with 'out' and stores the result in 'out'.
  static void Or(
      const MinMaxFilterPB& in, MinMaxFilterPB* out, const ColumnType& columnType);

  /// Copies the contents of 'in' into 'out'.
  static void Copy(const MinMaxFilterPB& in, MinMaxFilterPB* out);

  /// Returns the LLVM_CLASS_NAME for the given type.
  static std::string GetLlvmClassName(PrimitiveType type);

  /// Returns the IRFunction::Type for Insert() for the given type.
  static IRFunction::Type GetInsertIRFunctionType(ColumnType col_type);

  bool AlwaysTrue() const { return always_true_; }

  /// Compute and return the ratio of the filter overlapping with a range defined by
  /// [data_min, data_max] of type 'type' as follows.
  /// 1. If there is no overlapping, ratio = 0.0;
  /// 2. If the filter completely covers the data range, ratio = 1.0;
  /// 3. If the filter covers the data range to a certain degree,
  ///    ratio = (overlapped area) / (data area), where
  ///    overlapped area = min(filter_max, data_max) - max(filter_min, data_min) + 1
  ///    data area = data_max - data_min + 1
  virtual float ComputeOverlapRatio(
      const ColumnType& type, void* data_min, void* data_max) = 0;

 protected:
  /// Makes this filter always return true.
  virtual void SetAlwaysTrue() { always_true_ = true; }

  bool always_true_;
};

#define NUMERIC_MIN_MAX_FILTER(NAME, TYPE)                                             \
  class NAME##MinMaxFilter : public MinMaxFilter {                                     \
   public:                                                                             \
    NAME##MinMaxFilter() {                                                             \
      min_ = std::numeric_limits<TYPE>::max();                                         \
      max_ = std::numeric_limits<TYPE>::lowest();                                      \
    }                                                                                  \
    NAME##MinMaxFilter(const MinMaxFilterPB& protobuf);                                \
    virtual ~NAME##MinMaxFilter() {}                                                   \
    virtual const void* GetMin() const override { return &min_; }                      \
    virtual const void* GetMax() const override { return &max_; }                      \
    virtual bool GetCastIntMinMax(                                                     \
        const ColumnType& type, int64_t* out_min, int64_t* out_max) override;          \
    bool EvalOverlap(const ColumnType& type, void* data_min, void* data_max) override; \
    float ComputeOverlapRatio(                                                         \
        const ColumnType& type, void* data_min, void* data_max) override;              \
    virtual PrimitiveType type() override;                                             \
    virtual void Insert(const void* val) override;                                     \
    virtual bool AlwaysFalse() const override {                                        \
      return min_ == std::numeric_limits<TYPE>::max()                                  \
          && max_ == std::numeric_limits<TYPE>::lowest();                              \
    }                                                                                  \
    virtual void ToProtobuf(MinMaxFilterPB* protobuf) const override;                  \
    virtual std::string DebugString() const override;                                  \
    static void Or(const MinMaxFilterPB& in, MinMaxFilterPB* out);                     \
    static void Copy(const MinMaxFilterPB& in, MinMaxFilterPB* out);                   \
    static const char* LLVM_CLASS_NAME;                                                \
                                                                                       \
   private:                                                                            \
    bool EvalOverlap(const ColumnType& type, void* data_min, void* data_max,           \
        int64_t* filter_min64, int64_t* filter_max64);                                 \
                                                                                       \
   private:                                                                            \
    TYPE min_;                                                                         \
    TYPE max_;                                                                         \
  };

NUMERIC_MIN_MAX_FILTER(Bool, bool);
NUMERIC_MIN_MAX_FILTER(TinyInt, int8_t);
NUMERIC_MIN_MAX_FILTER(SmallInt, int16_t);
NUMERIC_MIN_MAX_FILTER(Int, int32_t);
NUMERIC_MIN_MAX_FILTER(BigInt, int64_t);
NUMERIC_MIN_MAX_FILTER(Float, float);
NUMERIC_MIN_MAX_FILTER(Double, double);

int64_t GetIntTypeValue(const ColumnType& type, const void* value);

class StringMinMaxFilter : public MinMaxFilter {
 public:
  StringMinMaxFilter(MemTracker* mem_tracker)
    : MinMaxFilter(),
      mem_pool_(mem_tracker),
      min_buffer_(&mem_pool_),
      max_buffer_(&mem_pool_),
      always_false_(true) {}
  StringMinMaxFilter(const MinMaxFilterPB& protobuf, MemTracker* mem_tracker);
  virtual ~StringMinMaxFilter() {}
  virtual void Close() override { mem_pool_.FreeAll(); }

  virtual const void* GetMin() const override { return &min_; }
  virtual const void* GetMax() const override { return &max_; }
  virtual PrimitiveType type() override;


  virtual void Insert(const void* val) override;
  virtual bool AlwaysFalse() const override { return always_false_; }
  bool EvalOverlap(
      const ColumnType& type, void* data_min, void* data_max) override;
  virtual float ComputeOverlapRatio(
      const ColumnType& type, void* data_min, void* data_max) override;

  /// Copies the values pointed to by 'min_'/'max_' into 'min_buffer_'/'max_buffer_',
  /// truncating them if necessary.
  virtual void MaterializeValues() override;

  virtual void ToProtobuf(MinMaxFilterPB* protobuf) const override;
  virtual std::string DebugString() const override;

  static void Or(const MinMaxFilterPB& in, MinMaxFilterPB* out);
  static void Copy(const MinMaxFilterPB& in, MinMaxFilterPB* out);

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 protected:
  virtual void SetAlwaysTrue() override;

 private:
  /// Copies the contents of 'value' into 'buffer', up to 'len', and reassignes 'value' to
  /// point to 'buffer'. If an oom is hit, disables the filter by setting 'always_true_'
  /// to true.
  void CopyToBuffer(StringBuffer* buffer, StringValue* value, int64_t len);

  /// The maximum length of string to store in 'min_str_' or 'max_str_'. Strings inserted
  /// into this filter that are longer than this will be truncated.
  static const int MAX_BOUND_LENGTH;

  /// MemPool that 'min_buffer_'/'max_buffer_' are allocated from.
  MemPool mem_pool_;

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
};

#define DATE_TIME_MIN_MAX_FILTER(NAME, TYPE)                                           \
  class NAME##MinMaxFilter : public MinMaxFilter {                                     \
   public:                                                                             \
    NAME##MinMaxFilter() { always_false_ = true; }                                     \
    NAME##MinMaxFilter(const MinMaxFilterPB& protobuf);                                \
    virtual ~NAME##MinMaxFilter() {}                                                   \
    virtual const void* GetMin() const override { return &min_; }                      \
    virtual const void* GetMax() const override { return &max_; }                      \
    virtual PrimitiveType type() override;                                             \
    virtual void Insert(const void* val) override;                                     \
    virtual bool AlwaysFalse() const override { return always_false_; }                \
    bool EvalOverlap(const ColumnType& type, void* data_min, void* data_max) override; \
    virtual float ComputeOverlapRatio(                                                 \
        const ColumnType& type, void* data_min, void* data_max) override;              \
    virtual void ToProtobuf(MinMaxFilterPB* protobuf) const override;                  \
    virtual std::string DebugString() const override;                                  \
    static void Or(const MinMaxFilterPB& in, MinMaxFilterPB* out);                     \
    static void Copy(const MinMaxFilterPB& in, MinMaxFilterPB* out);                   \
    static const char* LLVM_CLASS_NAME;                                                \
                                                                                       \
   private:                                                                            \
    TYPE min_;                                                                         \
    TYPE max_;                                                                         \
    /* True if no rows have been inserted. */                                          \
    bool always_false_;                                                                \
  };

DATE_TIME_MIN_MAX_FILTER(Timestamp, TimestampValue);
DATE_TIME_MIN_MAX_FILTER(Date, DateValue);

#define DECIMAL_SIZE_4BYTE 4
#define DECIMAL_SIZE_8BYTE 8
#define DECIMAL_SIZE_16BYTE 16

// body of GetMin and GetMax defined below
#pragma push_macro("GET_MINMAX")
#define GET_MINMAX(MIN_OR_MAX)                              \
  do {                                                      \
    switch (size_) {                                        \
      case DECIMAL_SIZE_4BYTE:                              \
        return &(MIN_OR_MAX##4_);                           \
        break;                                              \
      case DECIMAL_SIZE_8BYTE:                              \
        return &(MIN_OR_MAX##8_);                           \
        break;                                              \
      case DECIMAL_SIZE_16BYTE:                             \
        return &(MIN_OR_MAX##16_);                          \
        break;                                              \
      default:                                              \
        DCHECK(false) << "Unknown decimal size: " << size_; \
    }                                                       \
  } while (false)

// Decimal data can be stored using 4 bytes, 8 bytes or 16 bytes.  The filter
// is for a particular column, and the size needed is fixed based on the
// column specification.  So, a union is used to store the min and max value.
// The precision comes in as input.  Based on the precision the size is found and
// the respective variable will be used to store the value.  The code is
// macro-ized to handle different sized decimals.
class DecimalMinMaxFilter : public MinMaxFilter {
 public:
  DecimalMinMaxFilter(int precision)
    : size_(ColumnType::GetDecimalByteSize(precision)), always_false_(true) {}

  DecimalMinMaxFilter(const MinMaxFilterPB& protobuf, int precision);
  virtual ~DecimalMinMaxFilter() {}

  virtual const void* GetMin() const override {
    GET_MINMAX(min);
    return nullptr;
  }

  virtual const void* GetMax() const override {
    GET_MINMAX(max);
    return nullptr;
  }

  virtual void Insert(const void* val) override;
  virtual PrimitiveType type() override;
  virtual bool AlwaysFalse() const override { return always_false_; }
  virtual void ToProtobuf(MinMaxFilterPB* protobuf) const override;
  virtual std::string DebugString() const override;

  static void Or(const MinMaxFilterPB& in, MinMaxFilterPB* out, int precision);
  static void Copy(const MinMaxFilterPB& in, MinMaxFilterPB* out);
  bool EvalOverlap(
      const ColumnType& type, void* data_min, void* data_max) override;
  virtual float ComputeOverlapRatio(
      const ColumnType& type, void* data_min, void* data_max) override;

  void Insert4(const void* val);
  void Insert8(const void* val);
  void Insert16(const void* val);

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  const int size_;
  union {
    Decimal16Value min16_;
    Decimal8Value min8_;
    Decimal4Value min4_;
  };

  union {
    Decimal16Value max16_;
    Decimal8Value max8_;
    Decimal4Value max4_;
  };

  /// True if no rows have been inserted.
  bool always_false_;
};
#pragma pop_macro("GET_MINMAX")
}

#endif

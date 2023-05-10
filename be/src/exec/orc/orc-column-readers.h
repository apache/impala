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


#pragma once

#include <orc/OrcFile.hh>
#include <queue>

#include "exec/orc/hdfs-orc-scanner.h"
#include "exec/scratch-tuple-batch.h"

namespace impala {

class HdfsOrcScanner;

class OrcRowValidator {
 public:
  OrcRowValidator(const ValidWriteIdList& valid_write_ids) :
      valid_write_ids_(valid_write_ids) {}

  void UpdateTransactionBatch(orc::LongVectorBatch* batch) {
    write_ids_ = batch;
  }

  bool IsRowBatchValid() const;
 private:
  const ValidWriteIdList& valid_write_ids_;
  orc::LongVectorBatch* write_ids_;
};

/// Base class for reading an ORC column. Each column reader will keep track of an
/// orc::ColumnVectorBatch and transfer its values into Impala internals(tuples/slots).
///
/// We implement subclasses for each primitive types. They'll keep the SlotDescriptor
/// to locate the slot to materialize. Basically, the usage of the interfaces follows the
/// pattern:
///   reader1 = Create(orc_node1, slot_desc1, orc_scanner);
///   reader2 = Create(orc_node2, slot_desc2, orc_scanner);
///   while ( /* has new batch in the stripe */ ) {
///     reader1->UpdateInputBatch(orc_batch_of_column1)
///     reader2->UpdateInputBatch(orc_batch_of_column2)
///     while ( /* has more rows to read */ ) {
///       tuple = ...  // Init tuple
///       reader1->ReadValue(row_idx, tuple, mem_pool);
///       reader2->ReadValue(row_idx, tuple, mem_pool);
///       row_idx++;
///     }
///   }
///
/// The root reader is always an OrcStructReader, it drives the materialization of the
/// table level tuples, so we need more interface to deal with table/collection level
/// tuple materialization. See more in the class comments of OrcStructReader.
class OrcColumnReader {
 public:
  /// Create a column reader for the given 'slot_desc' based on the ORC 'node'. We say
  /// the 'slot_desc' and ORC 'node' match iff
  ///     scanner->slot_to_col_id_[slot_desc] == node->getColumnId()
  /// Caller should guarantee that 'slot_desc' matches to ORC 'node' or one of its
  /// descendants. If 'node' is a primitive type, 'slot_desc' should match it since
  /// primitive types don't have descendants.
  /// If 'node' is in complex types (struct/array/map) and does not match 'slot_desc',
  /// the created reader will use the 'slot_desc' to create its children. See more in
  /// constructors of complex column readers.
  /// The Create function adds the object to the obj_pool of the parent HdfsOrcScanner.
  static OrcColumnReader* Create(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  /// Base constructor for all types of readers that hold a SlotDescriptor (direct
  /// readers). Primitive column readers will materialize values into the slot. STRUCT
  /// column readers will delegate the slot materialization to its children. Collection
  /// column (ARRAY/MAP) readers will create CollectionValue in the slot and assemble
  /// collection tuples referenced by the CollectionValue. (See more in 'ReadValue')
  OrcColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  virtual ~OrcColumnReader() { }

  /// Default to true for primitive column readers. Only complex column readers can be
  /// not materializing tuples.
  virtual bool MaterializeTuple() const = 0;

  /// Whether it's a reader for a STRUCT/ARRAY/MAP column.
  virtual bool IsComplexColumnReader() const = 0;

  /// Whether it's a reader for a ARRAY/MAP column.
  virtual bool IsCollectionReader() const = 0;

  /// Update the orc batch we tracked. We'll read values from it.
  virtual Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch)
      WARN_UNUSED_RESULT = 0;

  /// Read value at 'row_idx' of the ColumnVectorBatch into a slot of the given 'tuple'.
  /// Use 'pool' to allocate memory in need. Depends on the UpdateInputBatch being called
  /// before (thus batch_ is updated)
  virtual Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool)
      WARN_UNUSED_RESULT = 0;

  /// Reads a batch of values from ColumnVectorBatch starting from 'row_idx' into
  /// 'scratch_batch' starting from 'scratch_batch_idx'. Use 'pool' to allocate memory
  /// in need.
  virtual Status ReadValueBatch(int row_idx, ScratchTupleBatch* scratch_batch,
      MemPool* pool, int scratch_batch_idx) WARN_UNUSED_RESULT = 0;

  /// Returns the number of tuples this reader directly or indirectry writes. E.g. if it
  /// is a primitive reader it returns 'batch_->numElements'. If it is a collection
  /// reader then it depends on whether it writes the tuple directly or indirectly. If it
  /// writes the tuple directly, i.e. it creates a collection value, then it also returns
  /// 'batch_->numElements' which is the number of collection values (lists, maps) it
  /// writes. On the other hand, if it writes the tuple indirectly, i.e. it delegates the
  /// writing to its child then it returns child->NumElements().
  /// E.g. if there's a column named 'arr', and its type is array<array<array<int>>>, and
  /// the query is
  /// "SELECT item from t.arr.item.item"
  /// then NumElements() for the top OrcListReader returns the number of elements in the
  /// OrcIntColumnReader in the bottom.
  virtual int NumElements() const = 0;

 protected:
  friend class OrcStructReader;
  friend class OrcCollectionReader;

  /// Returns the ORC type column id corresponding to the given slot descriptor.
  uint64_t GetColId(const SlotDescriptor* desc) const {
    auto it = scanner_->slot_to_col_id_.find(desc);
    DCHECK(it != scanner_->slot_to_col_id_.end());
    return it->second;
  }

  /// Returns the ORC type column id corresponding to the given tuple descriptor.
  uint64_t GetColId(const TupleDescriptor* desc) const {
    auto it = scanner_->tuple_to_col_id_.find(desc);
    DCHECK(it != scanner_->tuple_to_col_id_.end());
    return it->second;
  }

  /// Convenient field for debug. We can't keep the pointer of orc::Type since they'll be
  /// destroyed after orc::RowReader was released. Only keep the id orc::Type here.
  uint64_t orc_column_id_;

  /// If the reader is materializing a slot inside a tuple, the SlotDescriptor is kept.
  /// Otherwise (top level readers), 'slot_desc_' will be nullptr.
  const SlotDescriptor* slot_desc_;

  HdfsOrcScanner* scanner_;

  inline static bool IsNull(orc::ColumnVectorBatch* orc_batch, int row_idx) {
    return orc_batch->hasNulls && !orc_batch->notNull[row_idx];
  }

  /// Set the reader's slot in the given 'tuple' to NULL
  virtual void SetNullSlot(Tuple* tuple) {
    tuple->SetNull(DCHECK_NOTNULL(slot_desc_)->null_indicator_offset());
  }

  inline void* GetSlot(Tuple* tuple) const {
    return tuple->GetSlot(DCHECK_NOTNULL(slot_desc_)->tuple_offset());
  }
};

/// The main purpose of this class is to implement static polymorphism via the "curiously
/// recurring template pattern". All the derived classes are expected to provide a
/// subclass as the template parameter of this class. As a result the number of virtual
/// function calls can be reduced in ReadValueBatch() as we can directly call non-virtual
/// ReadValue() of the derived class.
template<class Final>
class OrcBatchedReader : public OrcColumnReader {
 public:
  OrcBatchedReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcColumnReader(node, slot_desc, scanner) {}

  Status ReadValueBatch(int row_idx, ScratchTupleBatch* scratch_batch, MemPool* pool,
      int scratch_batch_idx) override WARN_UNUSED_RESULT {
    Final* final = this->GetFinal();
    int num_to_read = std::min<int>(scratch_batch->capacity - scratch_batch_idx,
        final->NumElements() - row_idx);
    DCHECK_LE(row_idx + num_to_read, final->NumElements());
    for (int i = 0; i < num_to_read; ++i) {
      int scratch_batch_pos = i + scratch_batch_idx;
      uint8_t* next_tuple = scratch_batch->tuple_mem +
          scratch_batch_pos * OrcColumnReader::scanner_->tuple_byte_size();
      Tuple* tuple = reinterpret_cast<Tuple*>(next_tuple);
      // The compiler will devirtualize the call to ReadValue() if it is marked 'final' in
      // the 'Final' class. This way we can reduce the number of virtual function calls
      // to improve performance.
      RETURN_IF_ERROR(final->ReadValue(row_idx + i, tuple, pool));
    }
    scratch_batch->num_tuples = scratch_batch_idx + num_to_read;
    return Status::OK();
  }

  Final* GetFinal() { return static_cast<Final*>(this); }
  const Final* GetFinal() const { return static_cast<const Final*>(this); }
};

/// Base class for primitive types. It implements the common functions with 'final'
/// annotation. Template parameter 'Final' holds a concrete primitive column reader and is
/// propagated to OrcBatchedReader.
template<class Final>
class OrcPrimitiveColumnReader : public OrcBatchedReader<Final> {
 public:
  OrcPrimitiveColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcBatchedReader<Final>(node, slot_desc, scanner) {}
  virtual ~OrcPrimitiveColumnReader() {}

  bool IsComplexColumnReader() const final { return false; }

  bool IsCollectionReader() const final { return false; }

  bool MaterializeTuple() const final { return true; }

  int NumElements() const final {
    const Final* final = this->GetFinal();
    if (final->batch_ == nullptr) return 0;
    return final->batch_->numElements;
  }
};

class OrcBoolColumnReader : public OrcPrimitiveColumnReader<OrcBoolColumnReader> {
 public:
  OrcBoolColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcBoolColumnReader>(node, slot_desc, scanner) { }

  virtual ~OrcBoolColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::LongVectorBatch*>(orc_batch);
    // In debug mode, we use dynamic_cast<> to double-check the downcast is legal
    DCHECK(batch_ == dynamic_cast<orc::LongVectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;
 private:
  friend class OrcPrimitiveColumnReader<OrcBoolColumnReader>;

  orc::LongVectorBatch* batch_ = nullptr;
};

template<typename T>
class OrcIntColumnReader : public OrcPrimitiveColumnReader<OrcIntColumnReader<T>> {
 public:
  OrcIntColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcIntColumnReader<T>>(node, slot_desc, scanner) { }

  virtual ~OrcIntColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::LongVectorBatch*>(orc_batch);
    DCHECK(batch_ == static_cast<orc::LongVectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT {
    if (OrcColumnReader::IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
      OrcColumnReader::SetNullSlot(tuple);
      return Status::OK();
    }
    int64_t val = batch_->data.data()[row_idx];
    *(reinterpret_cast<T*>(OrcColumnReader::GetSlot(tuple))) = val;
    return Status::OK();
  }

 private:
  friend class OrcPrimitiveColumnReader<OrcIntColumnReader<T>>;

  orc::LongVectorBatch* batch_ = nullptr;
};

template<typename T>
class OrcDoubleColumnReader : public OrcPrimitiveColumnReader<OrcDoubleColumnReader<T>> {
 public:
  OrcDoubleColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcDoubleColumnReader<T>>(node, slot_desc, scanner) { }

  virtual ~OrcDoubleColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::DoubleVectorBatch*>(orc_batch);
    DCHECK(batch_ == dynamic_cast<orc::DoubleVectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT {
    if (OrcColumnReader::IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
      OrcColumnReader::SetNullSlot(tuple);
      return Status::OK();
    }
    double val = batch_->data.data()[row_idx];
    *(reinterpret_cast<T*>(OrcColumnReader::GetSlot(tuple))) = val;
    return Status::OK();
  }

 private:
  friend class OrcPrimitiveColumnReader<OrcDoubleColumnReader<T>>;

  orc::DoubleVectorBatch* batch_;
};

class OrcStringColumnReader : public OrcPrimitiveColumnReader<OrcStringColumnReader> {
 public:
  OrcStringColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcStringColumnReader>(node, slot_desc, scanner) { }

  virtual ~OrcStringColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::StringVectorBatch*>(orc_batch);
    if (orc_batch == nullptr) return Status::OK();
    // We update the blob of a non-encoded batch every time, but since the dictionary blob
    // is the same for the stripe, we only reset it for every new stripe.
    // Note that this is possible since the encoding should be the same for every batch
    // through the whole stripe.
    if(!orc_batch->isEncoded) {
      DCHECK(batch_ == dynamic_cast<orc::StringVectorBatch*>(orc_batch));
      return InitBlob(&batch_->blob, scanner_->data_batch_pool_.get());
    }
    DCHECK(static_cast<orc::EncodedStringVectorBatch*>(batch_) ==
        dynamic_cast<orc::EncodedStringVectorBatch*>(orc_batch));
    if (last_stripe_idx_ != scanner_->stripe_idx_) {
      last_stripe_idx_ = scanner_->stripe_idx_;
      auto current_batch = static_cast<orc::EncodedStringVectorBatch*>(batch_);
      return InitBlob(&current_batch->dictionary->dictionaryBlob,
          scanner_->dictionary_pool_.get());
    }
    return Status::OK();
  }

  /// Overrides the method to invoke templated ReadValueBatchInternal().
  Status ReadValueBatch(int row_idx, ScratchTupleBatch* scratch_batch,
      MemPool* pool, int scratch_batch_idx) override WARN_UNUSED_RESULT;

  /// Overrides ReadValue() to invoke templated ReadValueBatchInternal().
  /// It's only used in materializing collection (i.e. list/map) values.
  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) override WARN_UNUSED_RESULT;

 private:
  friend class OrcPrimitiveColumnReader<OrcStringColumnReader>;

  /// Template implementation for ReadValueBatch() to avoid checks inside the inner loop.
  /// When 'ENCODED' is true, the orc string vector batch is expected to be dictionary
  /// encoded.
  template<PrimitiveType SLOT_TYPE, bool ENCODED>
  Status ReadValueBatchInternal(int row_idx, ScratchTupleBatch* scratch_batch,
      int scratch_batch_idx) WARN_UNUSED_RESULT;

  /// Template implementation for ReadValue(). We use this method instead of ReadValue()
  /// in ReadValueBatch() so we can check types out of the loop.
  template<PrimitiveType SLOT_TYPE, bool ENCODED>
  ALWAYS_INLINE inline Status ReadValueInternal(int row_idx, Tuple* tuple)
      WARN_UNUSED_RESULT;

  orc::StringVectorBatch* batch_ = nullptr;
  // We copy the blob from the batch, so the memory will be handled by Impala, and not
  // by the ORC lib.
  char* blob_ = nullptr;

  // We cache the last stripe so we know when we have to update the blob (in case of
  // dictionary encoding).
  int last_stripe_idx_ = -1;

  /// Initializes the blob if it has not been already in the current batch.
  /// Unfortunately, this cannot be done in UpdateInputBatch, since we do not have
  /// access to the pool there.
  Status InitBlob(orc::DataBuffer<char>* blob, MemPool* pool);
};

class OrcTimestampReader : public OrcPrimitiveColumnReader<OrcTimestampReader> {
 public:
  OrcTimestampReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcTimestampReader>(node, slot_desc, scanner) {
    if (node->getKind() == orc::TIMESTAMP_INSTANT) {
      timezone_ = scanner_->state_->local_time_zone();
    }
  }

  virtual ~OrcTimestampReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::TimestampVectorBatch*>(orc_batch);
    DCHECK(batch_ == dynamic_cast<orc::TimestampVectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

 private:
  friend class OrcPrimitiveColumnReader<OrcTimestampReader>;

  orc::TimestampVectorBatch* batch_ = nullptr;
  const Timezone* timezone_ = UTCPTR;
};

class OrcDateColumnReader : public OrcPrimitiveColumnReader<OrcDateColumnReader> {
 public:
  OrcDateColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcDateColumnReader>(node, slot_desc, scanner) { }

  virtual ~OrcDateColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::LongVectorBatch*>(orc_batch);
    DCHECK(batch_ == dynamic_cast<orc::LongVectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

 private:
  friend class OrcPrimitiveColumnReader<OrcDateColumnReader>;

  orc::LongVectorBatch* batch_ = nullptr;
};

template<typename DECIMAL_TYPE>
class OrcDecimalColumnReader
    : public OrcPrimitiveColumnReader<OrcDecimalColumnReader<DECIMAL_TYPE>> {
 public:
  OrcDecimalColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcDecimalColumnReader<DECIMAL_TYPE>>(node, slot_desc,
          scanner) { }

  virtual ~OrcDecimalColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    // Reminder: even decimal(1,1) is stored in int64 batch
    batch_ = static_cast<orc::Decimal64VectorBatch*>(orc_batch);
    DCHECK(batch_ == dynamic_cast<orc::Decimal64VectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT {
    if (OrcColumnReader::IsNull(DCHECK_NOTNULL(batch_), row_idx)) {
      OrcColumnReader::SetNullSlot(tuple);
      return Status::OK();
    }
    int64_t val = batch_->values.data()[row_idx];
    reinterpret_cast<DECIMAL_TYPE*>(OrcColumnReader::GetSlot(tuple))->set_value(val);
    return Status::OK();
  }

 private:
  friend class OrcPrimitiveColumnReader<OrcDecimalColumnReader<DECIMAL_TYPE>>;

  orc::Decimal64VectorBatch* batch_ = nullptr;
};

class OrcDecimal16ColumnReader
    : public OrcPrimitiveColumnReader<OrcDecimal16ColumnReader> {
 public:
  OrcDecimal16ColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner)
      : OrcPrimitiveColumnReader<OrcDecimal16ColumnReader>(node, slot_desc, scanner) { }

  virtual ~OrcDecimal16ColumnReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    batch_ = static_cast<orc::Decimal128VectorBatch*>(orc_batch);
    DCHECK(batch_ == dynamic_cast<orc::Decimal128VectorBatch*>(orc_batch));
    return Status::OK();
  }

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

 private:
  friend class OrcPrimitiveColumnReader<OrcDecimal16ColumnReader>;

  orc::Decimal128VectorBatch* batch_ = nullptr;
};

/// Base class for reading a complex column. The two subclasses are OrcStructReader and
/// OrcCollectionReader. Each OrcComplexColumnReader has children readers for sub types.
/// Each slot maps to a child. Different children may maps to the same sub type, because
/// SlotDescriptors of a TupleDescriptor may have the same col_path (e.g. when there're
/// sub queries). The root reader is always an OrcStructReader since the root of the ORC
/// schema is represented as a STRUCT type.
///
/// For complex readers, they can be divided into two kinds by whether they should
/// materialize their tuples (reflected by materialize_tuple_).
///
/// For collection type readers that materialize a CollectionValue they create a
/// CollectionValueBuilder when 'ReadValue' is called. Then recursively delegate the
/// materialization of collection tuples to the child that matches the TupleDescriptor.
/// This child tracks the boundary of current collection and call 'ReadChildrenValue' to
/// assemble collection tuples. (See more in HdfsOrcScanner::AssembleCollection).
/// If they don't materialize collection values then they just delegate the reading to
/// their children.
///
/// Children readers are created in the constructor recursively.
class OrcComplexColumnReader : public OrcBatchedReader<OrcComplexColumnReader> {
 public:
  static OrcComplexColumnReader* CreateTopLevelReader(const orc::Type* node,
      const TupleDescriptor* tuple_desc, HdfsOrcScanner* scanner);

  /// Constructor for indirect readers, they delegate reads to their children.
  OrcComplexColumnReader(const orc::Type* node, const TupleDescriptor* table_tuple_desc,
      HdfsOrcScanner* scanner);

  /// Constructor for readers that write to a single slot of the given tuple. However,
  /// this single slot might be a CollectionValue, in which case it materializes new
  /// tuples for it which will be filled by its children.
  OrcComplexColumnReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner) : OrcBatchedReader(node, slot_desc, scanner) { }

  virtual ~OrcComplexColumnReader() { }

  bool IsComplexColumnReader() const override { return true; }

  bool MaterializeTuple() const override { return materialize_tuple_; }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT {
    vbatch_ = orc_batch;
    return Status::OK();
  }

  /// Num of tuples inside the 'row_idx'-th row. LIST/MAP types will have 0 to N tuples.
  /// STRUCT type will always have one tuple.
  virtual int GetNumChildValues(int row_idx) const = 0;

  /// Collection values (array items, map keys/values) are concatenated in the child's
  /// batch. Get the start offset of values inside the 'row_idx'-th collection.
  virtual int GetChildBatchOffset(int row_idx) const = 0;

  const vector<OrcColumnReader*>& children() const { return children_; }

  /// Returns true if this column reader writes to a slot directly. Returns false if it
  /// delegates its job to its children. It might still write a position slot though.
  bool DirectReader() const { return slot_desc_ != nullptr; }
 protected:
  /// Return target column id. For collection slots it returns the column id corresponding
  /// to the collection tuple descriptor
  uint64_t GetTargetColId(const SlotDescriptor* slot_desc) const;

  vector<OrcColumnReader*> children_;

  /// Holds the TupleDescriptor if we should materialize its tuples
  const TupleDescriptor* tuple_desc_ = nullptr;

  bool materialize_tuple_ = false;

  /// Convenient reference to 'batch_' of subclass.
  orc::ColumnVectorBatch* vbatch_ = nullptr;
};

/// Struct readers control the reading of struct fields.
/// The ORC library always return a struct reader to read the contents of a file. That's
/// why our root reader is always an OrcStructReader. The root reader controls the
/// materialization of the top-level tuples.
/// It provides an interface that is convenient to use by the scanner.
/// The usage pattern is more or less the following:
///   while ( /* has new batch in the stripe */ ) {
///     reader->UpdateInputBatch(orc_batch);
///     while (!reader->EndOfBatch()) {
///       InitScratchTuples();
///       reader->TopLevelReadValueBatch(scratch_batch, mem_pool);
///       TransferScratchTuples();
///     }
///   }
/// 'TopLevelReadValueBatch' don't require a row index since the root reader will keep
/// track of the row index.
class OrcStructReader : public OrcComplexColumnReader {
 public:
  /// Constructor for top level reader
  OrcStructReader(const orc::Type* node, const TupleDescriptor* table_tuple_desc,
      HdfsOrcScanner* scanner);

  /// Constructor for a slot that materializes all it's children. E.g. when a struct is
  /// given in the select list.
  OrcStructReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      const TupleDescriptor* children_tuple, HdfsOrcScanner* scanner);

  /// Constructor for a struct that is not mapped directly to a slot instead it refers to
  /// a descendant column.
  OrcStructReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  virtual ~OrcStructReader() { }

  bool IsCollectionReader() const override { return false; }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT;

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

  Status TopLevelReadValueBatch(ScratchTupleBatch* scratch_batch, MemPool* pool)
      WARN_UNUSED_RESULT;

  Status ReadValueBatch(int row_idx, ScratchTupleBatch* scratch_batch, MemPool* pool,
      int scratch_batch_idx) override WARN_UNUSED_RESULT;

  /// Whether we've finished reading the current orc batch.
  bool EndOfBatch();

  int GetNumChildValues(int row_idx) const final { return 1; }

  int GetChildBatchOffset(int row_idx) const override { return row_idx; }

  int NumElements() const final {
    if (MaterializeTuple()) {
      return vbatch_ ? vbatch_->numElements : 0;
    }
    DCHECK_EQ(children().size(), 1);
    OrcColumnReader* child = children()[0];
    return child->NumElements();
  }

  void SetFileRowIndex(int64_t file_row_idx) { file_row_idx_ = file_row_idx; }

 private:
  void FillVirtualRowIdColumn(ScratchTupleBatch* scratch_batch, int scratch_batch_idx,
      int num_rows);

  orc::StructVectorBatch* batch_ = nullptr;

  /// Field ids of the children reader
  std::vector<int> children_fields_;

  /// Keep row index if we're top level readers
  int row_idx_;

  /// File-level row index. Only set for original files, and only when ACID field 'rowid'
  /// is needed.
  int64_t file_row_idx_ = -1;

  int current_write_id_field_index_ = -1;
  std::unique_ptr<OrcRowValidator> row_validator_;

  void SetNullSlot(Tuple* tuple) override {
    for (OrcColumnReader* child : children_) child->SetNullSlot(tuple);
    tuple->SetNull(DCHECK_NOTNULL(slot_desc_)->null_indicator_offset());
  }

  void CreateChildForSlot(const orc::Type* curr_node, const SlotDescriptor* slot_desc);

  Status ReadAndValidateRows(ScratchTupleBatch* scratch_batch, MemPool* pool);
};

class OrcCollectionReader : public OrcComplexColumnReader {
 public:
  /// Constructor for top level reader
  OrcCollectionReader(const orc::Type* node, const TupleDescriptor* table_tuple_desc,
      HdfsOrcScanner* scanner) : OrcComplexColumnReader(node, table_tuple_desc, scanner)
      { }

  OrcCollectionReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  virtual ~OrcCollectionReader() { }

  bool IsCollectionReader() const override { return true; }

  /// Assemble the given 'tuple' by reading children values into it. The corresponding
  /// children values are in the 'row_idx'-th collection. Each collection (List/Map) may
  /// have variable number of tuples, we only read children values of the 'tuple_idx'-th
  /// tuple.
  virtual Status ReadChildrenValue(int row_idx, int tuple_idx, Tuple* tuple,
      MemPool* pool) const WARN_UNUSED_RESULT = 0;
 protected:
  void SetNullSlot(Tuple* tuple) override {
    if (slot_desc_ != nullptr) {
      OrcColumnReader::SetNullSlot(tuple);
      return;
    }
    for (OrcColumnReader* child : children_) child->SetNullSlot(tuple);
  }

  Status AssembleCollection(int row_idx, Tuple* tuple, MemPool* pool) WARN_UNUSED_RESULT;
};

class OrcListReader : public OrcCollectionReader {
 public:
  OrcListReader(const orc::Type* node, const TupleDescriptor* table_tuple_desc,
      HdfsOrcScanner* scanner);

  OrcListReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  virtual ~OrcListReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT;

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

  int GetNumChildValues(int row_idx) const final;

  int GetChildBatchOffset(int row_idx) const override;

  Status ReadChildrenValue(int row_idx, int tuple_idx, Tuple* tuple, MemPool* pool)
      const override WARN_UNUSED_RESULT;

  int NumElements() const final;
 private:
  Status SetPositionSlot(int row_idx, Tuple* tuple);

  orc::ListVectorBatch* batch_ = nullptr;
  const SlotDescriptor* pos_slot_desc_ = nullptr;
  /// Keeps track the list we are reading. It's needed for calculation the position
  /// value.
  int list_idx_ = 0;

  void CreateChildForSlot(const orc::Type* node, const SlotDescriptor* slot_desc);
};

class OrcMapReader : public OrcCollectionReader {
 public:
  OrcMapReader(const orc::Type* node, const TupleDescriptor* table_tuple_desc,
      HdfsOrcScanner* scanner);

  OrcMapReader(const orc::Type* node, const SlotDescriptor* slot_desc,
      HdfsOrcScanner* scanner);

  virtual ~OrcMapReader() { }

  Status UpdateInputBatch(orc::ColumnVectorBatch* orc_batch) override WARN_UNUSED_RESULT;

  Status ReadValue(int row_idx, Tuple* tuple, MemPool* pool) final WARN_UNUSED_RESULT;

  int GetNumChildValues(int row_idx) const final;

  int GetChildBatchOffset(int row_idx) const override;

  Status ReadChildrenValue(int row_idx, int tuple_idx, Tuple* tuple, MemPool* pool)
      const override WARN_UNUSED_RESULT;

  int NumElements() const final;
 private:
  orc::MapVectorBatch* batch_ = nullptr;
  vector<OrcColumnReader*> key_readers_;
  vector<OrcColumnReader*> value_readers_;

  void CreateChildForSlot(const orc::Type* orc_type, const SlotDescriptor* slot_desc);
};

} // namespace impala

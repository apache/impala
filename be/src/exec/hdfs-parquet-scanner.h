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


#ifndef IMPALA_EXEC_HDFS_PARQUET_SCANNER_H
#define IMPALA_EXEC_HDFS_PARQUET_SCANNER_H

#include "exec/hdfs-scanner.h"
#include "exec/parquet-common.h"

namespace impala {

class CollectionValueBuilder;
struct HdfsFileDesc;

/// This scanner parses Parquet files located in HDFS, and writes the content as tuples in
/// the Impala in-memory representation of data, e.g.  (tuples, rows, row batches).
/// For the file format spec, see: github.com/apache/parquet-format
///
/// ---- Schema resolution ----
/// Additional columns are allowed at the end in either the table or file schema (i.e.,
/// extra columns at the end of the schema or extra fields at the end of a struct).  If
/// there are extra columns in the file schema, they are simply ignored. If there are
/// extra in the table schema, we return NULLs for those columns (if they're
/// materialized).
///
/// ---- Disk IO ----
/// Parquet (and other columnar formats) use scan ranges differently than other formats.
/// Each materialized column maps to a single ScanRange per row group.  For streaming
/// reads, all the columns need to be read in parallel. This is done by issuing one
/// ScanRange (in IssueInitialRanges()) for the file footer per split.
/// ProcessSplit() is called once for each original split and determines the row groups
/// whose midpoints fall within that split. We use the mid-point to determine whether a
/// row group should be processed because if the row group size is less than or equal to
/// the split size, the mid point guarantees that we have at least 50% of the row group in
/// the current split. ProcessSplit() then computes the column ranges for these row groups
/// and submits them to the IoMgr for immediate scheduling (so they don't surface in
/// DiskIoMgr::GetNextRange()). Scheduling them immediately also guarantees they are all
/// read at once.
///
/// Like the other scanners, each parquet scanner object is one to one with a
/// ScannerContext. Unlike the other scanners though, the context will have multiple
/// streams, one for each column. Row groups are processed one at a time this way.
///
/// ---- Nested types ----
/// This scanner supports reading and materializing nested data. For a good overview of
/// how nested data is encoded, see blog.twitter.com/2013/dremel-made-simple-with-parquet.
/// For how SQL nested schemas are translated to parquet schemas, see
/// github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types.
///
/// Examples:
/// For these examples, we will use the following table definition:
/// tbl:
///   id                bigint
///   array_col         array<array<int>>
///
/// The table definition could correspond to the following parquet schema (note the
/// required 'id' field. If written by Impala, all non-repeated fields would be optional,
/// but we can read repeated fields as well):
///
/// required group record         d=0 r=0
///   req int64 id                d=0 r=0
///   opt group array_col (LIST)  d=1 r=0
///     repeated group list       d=2 r=1
///       opt group item (LIST)   d=3 r=1
///         repeated group list   d=4 r=2
///           opt int32 item      d=5 r=2
///
/// Each element in the schema has been annotated with the maximum def level and maximum
/// rep level corresponding to that element. Note that the repeated elements add a def
/// level. This distinguishes between 0 items (empty list) and more than 0 items
/// (non-empty list). The containing optional LIST element for each array determines
/// whether the whole list is null or non-null. Maps work the same way, the only
/// differences being that the repeated group contains two child fields ("key" and "value"
/// instead of "item"), and the outer element is annotated with MAP instead of LIST.
///
/// Only scalar schema elements are materialized in parquet files; internal nested
/// elements can be reconstructed using the def and rep levels. To illustrate this, here
/// is data containing every valid definition and repetition for the materialized int
/// 'item' element. The data records appear on the left, the encoded definition levels,
/// repetition levels, and values for the 'item' field appear on the right (the encoded
/// 'id' field is not shown).
///
/// record                       d r v
/// ------------------------------------
/// {id: 0, array_col: NULL}     0 0 -
/// {id: 1, array_col: []}       1 0 -
/// {id: 2, array_col: [NULL]}   2 0 -
/// {id: 3, array_col: [[]]}     3 0 -
/// {id: 4, array_col: [[NULL]]} 4 0 -
/// {id: 5, array_col: [[1,      5 0 1
///                      NULL],  4 2 -
///                     [2]]}    5 1 2
/// {id: 6, array_col: [[3]]}    5 0 3
///
/// * Example query 1:
///     select id, inner.item from tbl t, t.array_col outer, outer.item inner
///   Results from above sample data:
///     4,NULL
///     5,1
///     5,NULL
///     5,2
///     6,3
///
/// Descriptors:
///  Tuple(id=0 tuple_path=[] slots=[
///    Slot(id=0 type=ARRAY col_path=[1] collection_item_tuple_id=1),
///    Slot(id=2 type=BIGINT col_path=[0])])
///  Tuple(id=1 tuple_path=[1] slots=[
///    Slot(id=1 type=ARRAY col_path=[1,0] collection_item_tuple_id=2)])
///  Tuple(id=2 tuple_path=[1, 0] slots=[
///    Slot(id=3 type=INT col_path=[1,0,0])])
///
///   The parquet scanner will materialize the following in-memory row batch:
///          RowBatch
///        +==========+
///        | 0 | NULL |
///        |----------|
///        | 1 | NULL |      outer
///        |----------|     +======+
///        | 2 |  --------->| NULL |
///        |   |      |     +======+
///        |----------|
///        |   |      |     +======+
///        | 3 |  --------->| NULL |
///        |   |      |     +======+
///        |   |      |                  inner
///        |----------|     +======+    +======+
///        | 4 |  --------->|  -------->| NULL |
///        |   |      |     +======+    +======+
///        |   |      |
///        |----------|     +======+    +======+
///        | 5 |  --------->|  -------->|  1   |
///        |   |      |     |      |    +------+
///        |   |      |     |      |    | NULL |
///        |   |      |     +------+    +======+
///        |   |      |     |      |
///        |   |      |     |      |    +======+
///        |   |      |     |  -------->|  2   |
///        |   |      |     +======+    +======+
///        |   |      |
///        |----------|     +======+    +======+
///        | 6 |  --------->|  -------->|  3   |
///        +==========+     +======+    +======+
///
///   The top-level row batch contains two slots, one containing the int64_t 'id' slot and
///   the other containing the CollectionValue 'array_col' slot. The CollectionValues in
///   turn contain pointers to their item tuple data. Each item tuple contains a single
///   ArrayColumn slot ('array_col.item'). The inner CollectionValues' item tuples contain
///   a single int 'item' slot.
///
///   Note that the scanner materializes a NULL CollectionValue for empty collections.
///   This is technically a bug (it should materialize a CollectionValue with num_tuples =
///   0), but we don't distinguish between these two cases yet.
///   TODO: fix this (IMPALA-2272)
///
///   The column readers that materialize this structure form a tree analagous to the
///   materialized output:
///     CollectionColumnReader slot_id=0 node="repeated group list (d=2 r=1)"
///       CollectionColumnReader slot_id=1 node="repeated group list (d=4 r=2)"
///         ScalarColumnReader<int32_t> slot_id=3 node="opt int32 item (d=5 r=2)"
///     ScalarColumnReader<int64_t> slot_id=2 node="req int64 id (d=0 r=0)"
///
///   Note that the collection column readers reference the "repeated group item" schema
///   element of the serialized array, not the outer "opt group" element. This is what
///   causes the bug described above, it should consider both elements.
///
/// * Example query 2:
///     select inner.item from tbl.array_col.item inner;
///   Results from the above sample data:
///     NULL
///     1
///     NULL
///     2
///     3
///
///   Descriptors:
///    Tuple(id=0 tuple_path=[1, 0] slots=[
///      Slot(id=0 type=INT col_path=[1,0,0])])
///
///   In-memory row batch:
///     +======+
///     | NULL |
///     |------|
///     |  1   |
///     |------|
///     | NULL |
///     |------|
///     |  2   |
///     |------|
///     |  3   |
///     +======+
///
///   Column readers:
///     ScalarColumnReader<int32_t> slot_id=0 node="opt int32 item (d=5 r=2)"
///
///   In this example, the scanner doesn't materialize a nested in-memory result, since
///   only the single int 'item' slot is materialized. However, it still needs to read the
///   nested data as shown above. An important point to notice is that a tuple is not
///   materialized for every rep and def level pair read -- there are 9 of these pairs
///   total in the sample data above, but only 5 tuples are materialized. This is because
///   in this case, nothing should be materialized for NULL or empty arrays, since we're
///   only materializing the innermost item. If a def level is read that doesn't
///   correspond to any item value (NULL or otherwise), the scanner advances to the next
///   rep and def levels without materializing a tuple.
///
/// * Example query 3:
///     select id, inner.item from tbl t, t.array_col.item inner
///   Results from the above sample data (same as example 1):
///     4,NULL
///     5,1
///     5,NULL
///     5,2
///     6,3
///
///   Descriptors:
///    Tuple(id=0 tuple_path=[] slots=[
///      Slot(id=0 type=ARRAY col_path=[2]),
///      Slot(id=1 type=BIGINT col_path=[0])])
///    Tuple(id=1 tuple_path=[2, 0] slots=[
///      Slot(id=2 type=INT col_path=[2,0,0])])
///
///   In-memory row batch:
///       RowBatch
///     +==========+
///     | 0 | NULL |
///     |----------|
///     | 1 | NULL |
///     |----------|      inner
///     | 2 |  --------->+======+
///     |   |      |     +======+
///     |----------|
///     |   |      |
///     | 3 |  --------->+======+
///     |   |      |     +======+
///     |   |      |
///     |----------|     +======+
///     | 4 |  --------->| NULL |
///     |   |      |     +======+
///     |   |      |
///     |----------|     +======+
///     | 5 |  --------->|  1   |
///     |   |      |     +------+
///     |   |      |     | NULL |
///     |   |      |     +------+
///     |   |      |     |  2   |
///     |   |      |     +======+
///     |   |      |
///     |----------|     +======+
///     | 6 |  --------->|  3   |
///     +==========+     +======+
///
///   Column readers:
///     CollectionColumnReader slot_id=0 node="repeated group list (d=2 r=1)"
///       ScalarColumnReader<int32_t> slot_id=2 node="opt int32 item (d=5 r=2)"
///     ScalarColumnReader<int32_t> id=1 node="req int64 id (d=0 r=0)"
///
///   In this example, the scanner materializes a "flattened" version of inner, rather
///   than the full 3-level structure. Note that the collection reader references the
///   outer array, which determines how long each materialized array is, and the items in
///   the array are from the inner array.

class HdfsParquetScanner : public HdfsScanner {
 public:
  HdfsParquetScanner(HdfsScanNode* scan_node, RuntimeState* state);

  virtual ~HdfsParquetScanner();
  virtual Status Prepare(ScannerContext* context);
  virtual void Close();
  virtual Status ProcessSplit();

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want.
  static Status IssueInitialRanges(HdfsScanNode* scan_node,
                                   const std::vector<HdfsFileDesc*>& files);

  struct FileVersion {
    /// Application that wrote the file. e.g. "IMPALA"
    std::string application;

    /// Version of the application that wrote the file, expressed in three parts
    /// (<major>.<minor>.<patch>). Unspecified parts default to 0, and extra parts are
    /// ignored. e.g.:
    /// "1.2.3"    => {1, 2, 3}
    /// "1.2"      => {1, 2, 0}
    /// "1.2-cdh5" => {1, 2, 0}
    struct {
      int major;
      int minor;
      int patch;
    } version;

    /// If true, this file was generated by an Impala internal release
    bool is_impala_internal;

    FileVersion() : is_impala_internal(false) { }

    /// Parses the version from the created_by string
    FileVersion(const std::string& created_by);

    /// Returns true if version is strictly less than <major>.<minor>.<patch>
    bool VersionLt(int major, int minor = 0, int patch = 0) const;

    /// Returns true if version is equal to <major>.<minor>.<patch>
    bool VersionEq(int major, int minor, int patch) const;
  };

 private:
  /// Internal representation of a column schema (including nested-type columns).
  struct SchemaNode {
    /// The corresponding schema element defined in the file metadata
    const parquet::SchemaElement* element;

    /// The index into the RowGroup::columns list if this column is materialized in the
    /// file (i.e. it's a scalar type). -1 for nested types.
    int col_idx;

    /// The maximum definition level of this column, i.e., the definition level that
    /// corresponds to a non-NULL value. Valid values are >= 0.
    int max_def_level;

    /// The maximum repetition level of this column. Valid values are >= 0.
    int max_rep_level;

    /// The definition level of the most immediate ancestor of this node with repeated
    /// field repetition type. 0 if there are no repeated ancestors.
    int def_level_of_immediate_repeated_ancestor;

    /// Any nested schema nodes. Empty for non-nested types.
    std::vector<SchemaNode> children;

    SchemaNode() : element(NULL), col_idx(-1), max_def_level(-1), max_rep_level(-1),
                   def_level_of_immediate_repeated_ancestor(-1) { }

    std::string DebugString(int indent = 0) const;

    bool is_repeated() const {
      return element->repetition_type == parquet::FieldRepetitionType::REPEATED;
    }
  };

  /// Size of the file footer.  This is a guess.  If this value is too little, we will
  /// need to issue another read.
  static const int64_t FOOTER_SIZE;

  /// Class that implements Parquet definition and repetition level decoding.
  class LevelDecoder;

  /// Per column reader.
  class ColumnReader;
  friend class ColumnReader;

  class CollectionColumnReader;
  friend class CollectionColumnReader;

  class BaseScalarColumnReader;
  friend class BaseScalarColumnReader;

  template<typename T, bool MATERIALIZED> class ScalarColumnReader;
  template<typename T, bool MATERIALIZED> friend class ScalarColumnReader;
  class BoolColumnReader;
  friend class BoolColumnReader;

  /// Column reader for each materialized columns for this file.
  std::vector<ColumnReader*> column_readers_;

  /// File metadata thrift object
  parquet::FileMetaData file_metadata_;

  /// Version of the application that wrote this file.
  FileVersion file_version_;

  /// The root schema node for this file
  SchemaNode schema_;

  /// Scan range for the metadata.
  const DiskIoMgr::ScanRange* metadata_range_;

  /// Pool to copy dictionary page buffer into. This pool is shared across all the
  /// pages in a column chunk.
  boost::scoped_ptr<MemPool> dictionary_pool_;

  /// Timer for materializing rows.  This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Number of columns that need to be read.
  RuntimeProfile::Counter* num_cols_counter_;

  /// Number of row groups that need to be read.
  RuntimeProfile::Counter* num_row_groups_counter_;

  const char* filename() const { return metadata_range_->file(); }

  /// Reads data using 'column_readers' to materialize instances of 'tuple_desc'
  /// (including recursively reading collections).
  ///
  /// If reading into a collection, 'coll_value_builder' should be non-NULL and
  /// 'new_collection_rep_level' set appropriately. Otherwise, 'coll_value_builder'
  /// should be NULL and 'new_collection_rep_level' should be -1.
  ///
  /// Returns when the row group is complete, the end of the current collection is reached
  /// as indicated by 'new_collection_rep_level' (if materializing a collection), or
  /// some other condition causes execution to halt (e.g. parse_error_ set, cancellation).
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  ///
  /// 'row_group_idx' is used for error checking when this is called on the table-level
  /// tuple. If reading into a collection, 'row_group_idx' doesn't matter.
  ///
  /// IN_COLLECTION is true if the columns we are materializing are part of a Parquet
  /// collection. MATERIALIZING_COLLECTION is true if we are materializing tuples inside
  /// a nested collection.
  template <bool IN_COLLECTION, bool MATERIALIZING_COLLECTION>
  bool AssembleRows(const TupleDescriptor* tuple_desc,
      const std::vector<ColumnReader*>& column_readers, int new_collection_rep_level,
      int row_group_idx, CollectionValueBuilder* coll_value_builder);

  /// Function used by AssembleRows() to read a single row into 'tuple'. Returns false if
  /// execution should be aborted for some reason, otherwise returns true.
  /// materialize_tuple is an in/out parameter. It is set to true by the caller to
  /// materialize the tuple.  If any conjuncts fail, materialize_tuple is set to false
  /// by ReadRow().
  /// 'tuple_materialized' is an output parameter set by this function. If false is
  /// returned, there are no guarantees about 'materialize_tuple' or the state of
  /// column_readers, so execution should be halted immediately.
  /// The template argument IN_COLLECTION allows an optimized version of this code to
  /// be produced in the case when we are materializing the top-level tuple.
  template <bool IN_COLLECTION>
  inline bool ReadRow(const std::vector<ColumnReader*>& column_readers, Tuple* tuple,
      MemPool* pool, bool* materialize_tuple);

  /// Find and return the last split in the file if it is assigned to this scan node.
  /// Returns NULL otherwise.
  static DiskIoMgr::ScanRange* FindFooterSplit(HdfsFileDesc* file);

  /// Validate column offsets by checking if the dictionary page comes before the data
  /// pages and checking if the column offsets lie within the file.
  Status ValidateColumnOffsets(const parquet::RowGroup& row_group);

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  /// *eosr is a return value.  If true, the scan range is complete (e.g. select count(*))
  Status ProcessFooter(bool* eosr);

  /// Populates 'column_readers' for the slots in 'tuple_desc', including creating child
  /// readers for any collections. Schema resolution is handled in this function as
  /// well. Fills in the appropriate template tuple slot with NULL for any materialized
  /// fields missing in the file.
  Status CreateColumnReaders(const TupleDescriptor& tuple_desc,
      std::vector<ColumnReader*>* column_readers);

  /// Returns the total number of scalar column readers in 'column_readers', including
  /// the children of collection readers.
  int CountScalarColumns(const std::vector<ColumnReader*>& column_readers);

  /// Creates a column reader for 'node'. slot_desc may be NULL, in which case the
  /// returned column reader can only be used to read def/rep levels.
  /// 'is_collection_field' should be set to true if the returned reader is reading a
  /// collection. This cannot be determined purely by 'node' because a repeated scalar
  /// node represents both an array and the array's items (in this case
  /// 'is_collection_field' should be true if the reader reads one value per array, and
  /// false if it reads one value per item).  The reader is added to the runtime state's
  /// object pool. Does not create child readers for collection readers; these must be
  /// added by the caller.
  ColumnReader* CreateReader(const SchemaNode& node, bool is_collection_field,
      const SlotDescriptor* slot_desc);

  /// Creates a column reader that reads one value for each item in the table or
  /// collection element corresponding to 'parent_path'. 'parent_path' should point to
  /// either a collection element or the root schema (i.e. empty path). The returned
  /// reader has no slot desc associated with it, meaning only NextLevels() and not
  /// ReadValue() can be called on it.
  ///
  /// This is used for counting item values, rather than materializing any values. For
  /// example, in a count(*) over a collection, there are no values to materialize, but we
  /// still need to iterate over every item in the collection to count them.
  Status CreateCountingReader(
      const SchemaPath& parent_path, ColumnReader** reader);

  /// Walks file_metadata_ and initiates reading the materialized columns.  This
  /// initializes 'column_readers' and issues the reads for the columns. 'column_readers'
  /// should be the readers used to materialize a single tuple (i.e., column_readers_ or
  /// the children of a collection node).
  Status InitColumns(
      int row_group_idx, const std::vector<ColumnReader*>& column_readers);

  /// Validates the file metadata
  Status ValidateFileMetadata();

  /// Validates the column metadata to make sure this column is supported (e.g. encoding,
  /// type, etc) and matches the type of col_reader's slot desc.
  Status ValidateColumn(const BaseScalarColumnReader& col_reader, int row_group_idx);

  /// Performs some validation once we've reached the end of a row group to help detect
  /// bugs or bad input files.
  Status ValidateEndOfRowGroup(const std::vector<ColumnReader*>& column_readers,
      int row_group_idx, int64_t rows_read);

  /// Part of the HdfsScanner interface, not used in Parquet.
  Status InitNewRange() { return Status::OK(); };

  /// Unflattens the schema metadata from a Parquet file metadata and converts it to our
  /// SchemaNode representation. Returns the result in 'n' unless an error status is
  /// returned. Does not set the slot_desc field of any SchemaNode.
  Status CreateSchemaTree(const std::vector<parquet::SchemaElement>& schema,
      SchemaNode* node) const;

  /// Recursive implementation used internally by the above CreateSchemaTree() function.
  Status CreateSchemaTree(const std::vector<parquet::SchemaElement>& schema,
      int max_def_level, int max_rep_level, int ira_def_level, int* idx, int* col_idx,
      SchemaNode* node) const;

  /// Traverses 'schema_' according to 'path', returning the result in 'node'. If 'path'
  /// does not exist in this file's schema, 'missing_field' is set to true and
  /// Status::OK() is returned, otherwise 'missing_field' is set to false. If 'path'
  /// resolves to a collecton position field, *pos_field is set to true. Otherwise
  /// 'pos_field' is set to false. Returns a non-OK status if 'path' cannot be resolved
  /// against the file's schema (e.g., unrecognized collection schema).
  ///
  /// Tries to resolve assuming either two- or three-level array encoding in
  /// 'schema_'. Returns a bad status if resolution fails in both cases.
  Status ResolvePath(const SchemaPath& path, SchemaNode** node, bool* pos_field,
      bool* missing_field);

  /// The 'array_encoding' parameter determines whether to assume one-, two-, or
  /// three-level array encoding. The returned status is not logged (i.e. it's an expected
  /// error).
  enum ArrayEncoding {
    ONE_LEVEL,
    TWO_LEVEL,
    THREE_LEVEL
  };
  Status ResolvePathHelper(ArrayEncoding array_encoding, const SchemaPath& path,
      SchemaNode** node, bool* pos_field, bool* missing_field);

  /// Helper functions for ResolvePathHelper().

  /// Advances 'node' to one of its children based on path[next_idx]. Returns the child
  /// node or sets 'missing_field' to true.
  SchemaNode* NextSchemaNode(const SchemaPath& path, int next_idx, SchemaNode* node,
    bool* missing_field);

  /// The ResolvePathHelper() logic for arrays.
  Status ResolveArray(ArrayEncoding array_encoding, const SchemaPath& path, int idx,
    SchemaNode** node, bool* pos_field, bool* missing_field);

  /// The ResolvePathHelper() logic for maps.
  Status ResolveMap(const SchemaPath& path, int idx, SchemaNode** node,
      bool* missing_field);

  /// The ResolvePathHelper() logic for scalars (just does validation since there's no
  /// more actual work to be done).
  Status ValidateScalarNode(const SchemaNode& node, const ColumnType& col_type,
      const SchemaPath& path, int idx);
};

} // namespace impala

#endif

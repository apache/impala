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


#ifndef IMPALA_EXEC_HDFS_PARQUET_SCANNER_H
#define IMPALA_EXEC_HDFS_PARQUET_SCANNER_H

#include "codegen/impala-ir.h"
#include "exec/hdfs-scanner.h"
#include "exec/parquet/parquet-column-stats.h"
#include "exec/parquet/parquet-common.h"
#include "exec/parquet/parquet-metadata-utils.h"
#include "exec/parquet/parquet-page-index.h"
#include "exec/parquet/parquet-scratch-tuple-batch.h"
#include "runtime/scoped-buffer.h"
#include "util/runtime-profile-counters.h"

namespace impala {

class CollectionValueBuilder;
struct HdfsFileDesc;

/// Internal schema representation and resolution.
struct SchemaNode;

/// Class that implements Parquet definition and repetition level decoding.
class ParquetLevelDecoder;

/// Per column reader.
class ParquetColumnReader;
class CollectionColumnReader;
class ColumnStatsReader;
class BaseScalarColumnReader;
template<typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
class ScalarColumnReader;
class BoolColumnReader;

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
/// RequestContext::GetNextUnstartedRange()). Scheduling them immediately also guarantees
/// they are all read at once.
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
///   The column readers that materialize this structure form a tree analogous to the
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
///
/// ---- Slot materialization ----
/// Top-level tuples:
/// The slots of top-level tuples are populated in a column-wise fashion. Each column
/// reader materializes a batch of values into a temporary 'scratch batch'. Once a
/// scratch batch has been fully populated, runtime filters and conjuncts are evaluated
/// against the scratch tuples, and the surviving tuples are set in the output batch that
/// is handed to the scan node. The ownership of tuple memory is transferred from a
/// scratch batch to an output row batch once all tuples in the scratch batch have either
/// been filtered or returned as part of an output batch.
///
/// Collection items:
/// Unlike the top-level tuples, the item tuples of CollectionValues are populated in
/// a row-wise fashion because doing it column-wise has the following challenges.
/// First, we would need to allocate a scratch batch for every collection-typed slot
/// which could consume a lot of memory. Then we'd need a similar mechanism to transfer
/// tuples that survive conjuncts to an output collection. However, CollectionValues lack
/// the row indirection that row batches have, so we would need to either deep copy the
/// surviving tuples, or come up with a different mechanism altogether.
/// TODO: Populating CollectionValues in a column-wise fashion seems different enough
/// and less critical for most of our users today to defer this task until later.
///
/// ---- Runtime filters ----
/// HdfsParquetScanner is able to apply runtime filters that arrive before or during
/// scanning. Filters are applied at both the row group (see AssembleRows()) and row (see
/// ReadRow()) scope. If all filter predicates do not pass, the row or row group will be
/// excluded from output. Only partition-column filters are applied at AssembleRows(). The
/// FilterContexts for these filters are cloned from the parent scan node and attached to
/// the ScannerContext.
///
/// ---- Page filtering ----
/// A Parquet file can contain a so called "page index". It has two parts, a column index
/// and an offset index. The column index contains statistics like minimum and maximum
/// values for each page. The offset index contains information about page locations in
/// the Parquet file and top-level row ranges. HdfsParquetScanner evaluates the min/max
/// conjuncts against the column index and determines the surviving pages with the help of
/// the offset index. Then it will configure the column readers to only scan the pages
/// and row ranges that have a chance to store rows that pass the conjuncts.
class HdfsParquetScanner : public HdfsScanner {
 public:
  HdfsParquetScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsParquetScanner() {}

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want. 'files' must not be empty.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
                                   const std::vector<HdfsFileDesc*>& files)
                                   WARN_UNUSED_RESULT;

  virtual Status Open(ScannerContext* context) WARN_UNUSED_RESULT;
  virtual Status ProcessSplit() WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch);

  /// Codegen ProcessScratchBatch(). Stores the resulting function in
  /// 'process_scratch_batch_fn' if codegen was successful or NULL otherwise.
  static Status Codegen(HdfsScanNodeBase* node,
      const std::vector<ScalarExpr*>& conjuncts,
      llvm::Function** process_scratch_batch_fn)
      WARN_UNUSED_RESULT;

  /// Helper function to create ColumnStatsReader object. 'col_order' might be NULL.
  ColumnStatsReader CreateColumnStatsReader(
      const parquet::ColumnChunk& col_chunk, const ColumnType& col_type,
      const parquet::ColumnOrder* col_order, const parquet::SchemaElement& element);

  /// Initializes a ParquetTimestampDecoder depending on writer, timezone, and the schema
  /// of the column.
  ParquetTimestampDecoder CreateTimestampDecoder(const parquet::SchemaElement& element);

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 private:
  friend class ParquetColumnReader;
  friend class CollectionColumnReader;
  friend class BaseScalarColumnReader;
  template<typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
  friend class ScalarColumnReader;
  friend class BoolColumnReader;
  friend class HdfsParquetScannerTest;
  friend class ParquetPageIndex;

  /// Index of the current row group being processed. Initialized to -1 which indicates
  /// that we have not started processing the first row group yet (GetNext() has not yet
  /// been called).
  int32_t row_group_idx_;

  /// Counts the number of rows processed for the current row group.
  int64_t row_group_rows_read_;

  /// Indicates whether we should advance to the next row group in the next GetNext().
  /// Starts out as true to move to the very first row group.
  bool advance_row_group_;

  boost::scoped_ptr<ParquetSchemaResolver> schema_resolver_;

  /// Tuple to hold values when reading parquet::Statistics. Owned by perm_pool_.
  Tuple* min_max_tuple_;

  /// Clone of Min/max statistics conjunct evaluators. Has the same life time as
  /// the scanner. Stored in 'obj_pool_'.
  vector<ScalarExprEvaluator*> min_max_conjunct_evals_;

  /// Pool used for allocating caches of definition/repetition levels and tuples for
  /// dictionary filtering. The definition/repetition levels are populated by the
  /// level readers. The pool is freed in Close().
  boost::scoped_ptr<MemPool> perm_pool_;

  /// Number of scratch batches processed so far.
  int64_t row_batches_produced_;

  /// Column reader for each top-level materialized slot in the output tuple.
  std::vector<ParquetColumnReader*> column_readers_;

  /// Column readers will write slot values into this scratch batch for
  /// top-level tuples. See AssembleRows().
  boost::scoped_ptr<ScratchTupleBatch> scratch_batch_;

  /// File metadata thrift object
  parquet::FileMetaData file_metadata_;

  /// Version of the application that wrote this file.
  ParquetFileVersion file_version_;

  /// Scan range for the metadata.
  const io::ScanRange* metadata_range_;

  /// Pool to copy dictionary page buffer into. This pool is shared across all the
  /// pages in a column chunk.
  boost::scoped_ptr<MemPool> dictionary_pool_;

  /// Contains the leftover ranges after evaluating the page index.
  /// If all rows were eliminated, then the row group is skipped immediately after
  /// evaluating the page index.
  std::vector<RowRange> candidate_ranges_;

  /// Column readers that are eligible for dictionary filtering.
  /// These are pointers to elements of column_readers_. Materialized columns that are
  /// dictionary encoded correspond to scalar columns that are either top-level columns
  /// or nested within a collection. CollectionColumnReaders are not eligible for
  /// dictionary filtering so are not included.
  std::vector<BaseScalarColumnReader*> dict_filterable_readers_;

  /// Column readers that are not eligible for dictionary filtering.
  /// These are pointers to elements of column_readers_. The readers are either top-level
  /// or nested within a collection.
  std::vector<BaseScalarColumnReader*> non_dict_filterable_readers_;

  /// Flattened list of all scalar column readers in column_readers_.
  std::vector<BaseScalarColumnReader*> scalar_readers_;

  /// Flattened collection column readers that point to readers in column_readers_.
  std::vector<CollectionColumnReader*> collection_readers_;

  /// Mapping from Parquet column indexes to scalar readers.
  std::unordered_map<int, BaseScalarColumnReader*> scalar_reader_map_;

  /// Memory used to store the tuples used for dictionary filtering. Tuples owned by
  /// perm_pool_.
  std::unordered_map<const TupleDescriptor*, Tuple*> dict_filter_tuple_map_;

  /// Timer for materializing rows.  This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Average and min/max time spent processing the footer by each split.
  RuntimeProfile::SummaryStatsCounter* process_footer_timer_stats_;

  /// Average and min/max time spent processing the page index for each row group.
  RuntimeProfile::SummaryStatsCounter* process_page_index_stats_;

  /// Number of columns that need to be read.
  RuntimeProfile::Counter* num_cols_counter_;

  /// Number of row groups that are skipped because of Parquet statistics, either by
  /// row group level statistics, or page level statistics.
  RuntimeProfile::Counter* num_stats_filtered_row_groups_counter_;

  /// Number of row groups that need to be read.
  RuntimeProfile::Counter* num_row_groups_counter_;

  /// Number of row groups with page index.
  RuntimeProfile::Counter* num_row_groups_with_page_index_counter_;

  /// Number of pages that are skipped because of Parquet page statistics.
  RuntimeProfile::Counter* num_stats_filtered_pages_counter_;

  /// Number of pages need to be examined. We need to scan
  /// 'num_pages_counter_ - num_stats_filtered_pages_counter_' pages.
  RuntimeProfile::Counter* num_pages_counter_;

  /// Number of scanners that end up doing no reads because their splits don't overlap
  /// with the midpoint of any row-group in the file.
  RuntimeProfile::Counter* num_scanners_with_no_reads_counter_;

  /// Number of row groups skipped due to dictionary filter
  RuntimeProfile::Counter* num_dict_filtered_row_groups_counter_;

  /// Tracks the size of any compressed pages read. If no compressed pages are read, this
  /// counter is empty
  RuntimeProfile::SummaryStatsCounter* parquet_compressed_page_size_counter_;

  /// Tracks the size of any de-compressed pages reads. This counter is updated in two
  /// places: (1) when a compressed page is de-compressed, the de-compressed size is added
  /// to this counter, (2) when a page that is not compressed is read, its size is added
  /// to this counter
  RuntimeProfile::SummaryStatsCounter* parquet_uncompressed_page_size_counter_;

  /// Number of collection items read in current row batch. It is a scanner-local counter
  /// used to reduce the frequency of updating HdfsScanNode counter. It is updated by the
  /// callees of AssembleRows() and is merged into the HdfsScanNode counter at the end of
  /// AssembleRows() and then is reset to 0.
  int64_t coll_items_read_counter_;

  typedef int (*ProcessScratchBatchFn)(HdfsParquetScanner*, RowBatch*);
  /// The codegen'd version of ProcessScratchBatch() if available, NULL otherwise.
  ProcessScratchBatchFn codegend_process_scratch_batch_fn_;

  ParquetPageIndex page_index_;

  const char* filename() const { return metadata_range_->file(); }

  virtual Status GetNextInternal(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Evaluates the min/max predicates of the 'scan_node_' using the parquet::Statistics
  /// of 'row_group'. 'file_metadata' is used to determine the ordering that was used to
  /// compute the statistics. Sets 'skip_row_group' to true if the row group can be
  /// skipped, 'false' otherwise.
  Status EvaluateStatsConjuncts(const parquet::FileMetaData& file_metadata,
      const parquet::RowGroup& row_group, bool* skip_row_group) WARN_UNUSED_RESULT;

  /// Advances 'row_group_idx_' to the next non-empty row group and initializes
  /// the column readers to scan it. Recoverable errors are logged to the runtime
  /// state. Only returns a non-OK status if a non-recoverable error is encountered
  /// (or abort_on_error is true). If OK is returned, 'parse_status_' is guaranteed
  /// to be OK as well.
  Status NextRowGroup() WARN_UNUSED_RESULT;

  /// High-level function for initializing page filtering for the scalar readers.
  /// Sets 'filter_pages' to true if found any page to filter out.
  Status ProcessPageIndex(bool* filter_pages);

  /// Evaluates 'min_max_conjunct_evals_' against the column index and determines the row
  /// ranges that might contain data we are looking for.
  /// Sets 'filter_pages' to true if found any page to filter out.
  Status EvaluatePageIndex(bool* filter_pages);

  /// Based on 'candidate_ranges_' it determines the candidate pages for each
  /// scalar reader.
  Status ComputeCandidatePagesForColumns(bool* filter_pages);

  /// Check that the scalar readers agree on the top-level row being scanned.
  Status CheckPageFiltering();

  /// Reads statistics data for a page of given column. Page is specified by 'page_idx',
  /// column is specified by 'column_index'. 'stats_field' specifies whether we should
  /// read the min or max value. 'is_null_page' is set to true for null pages, in that
  /// case there is no min nor max value. Returns true if the read of the min or max
  /// value was successful, in this case 'slot' will hold the min or max value.
  static bool ReadStatFromIndex(const ColumnStatsReader& stats_reader,
      const parquet::ColumnIndex& column_index, int page_idx,
      ColumnStatsReader::StatsField stats_field, bool* is_null_page, void* slot);

  /// Reads data using 'column_readers' to materialize top-level tuples into 'row_batch'.
  /// Returns a non-OK status if a non-recoverable error was encountered and execution
  /// of this query should be terminated immediately.
  /// May set *skip_row_group to indicate that the current row group should be skipped,
  /// e.g., due to a parse error, but execution should continue.
  Status AssembleRows(const std::vector<ParquetColumnReader*>& column_readers,
      RowBatch* row_batch, bool* skip_row_group) WARN_UNUSED_RESULT;

  /// Commit num_rows to the given row batch.
  /// Returns OK if the query is not cancelled and hasn't exceeded any mem limits.
  /// Scanner can call this with 0 rows to flush any pending resources (attached pools
  /// and io buffers) to minimize memory consumption.
  Status CommitRows(RowBatch* dst_batch, int num_rows) WARN_UNUSED_RESULT;

  /// Evaluates runtime filters and conjuncts (if any) against the tuples in
  /// 'scratch_batch_', and adds the surviving tuples to the given batch.
  /// Transfers the ownership of tuple memory to the target batch when the
  /// scratch batch is exhausted.
  /// Returns the number of rows that should be committed to the given batch.
  int TransferScratchTuples(RowBatch* dst_batch);

  /// Processes a single row batch for TransferScratchTuples, looping over scratch_batch_
  /// until it is exhausted or the output is full. Called for the case when there are
  /// materialized tuples. This is a separate function so it can be codegened.
  int ProcessScratchBatch(RowBatch* dst_batch);

  /// Reads data using 'column_readers' to materialize the tuples of a CollectionValue
  /// allocated from 'coll_value_builder'. Increases 'coll_items_read_counter_' by the
  /// number of items in this collection and descendant collections.
  ///
  /// 'new_collection_rep_level' indicates when the end of the collection has been
  /// reached, namely when current_rep_level <= new_collection_rep_level.
  ///
  /// Returns true when the end of the current collection is reached, and execution can
  /// be safely resumed.
  /// Returns false if execution should be aborted due to:
  /// - parse_error_ is set
  /// - query is cancelled
  /// - scan node limit was reached
  /// When false is returned the column_readers are left in an undefined state and
  /// execution should be aborted immediately by the caller.
  bool AssembleCollection(const std::vector<ParquetColumnReader*>& column_readers,
      int new_collection_rep_level, CollectionValueBuilder* coll_value_builder);

  /// Function used by AssembleCollection() to materialize a single collection item
  /// into 'tuple'. Returns false if execution should be aborted for some reason,
  /// otherwise returns true.
  /// If 'materialize_tuple' is false, only advances the column readers' levels,
  /// and does not read any data values.
  inline bool ReadCollectionItem(const std::vector<ParquetColumnReader*>& column_readers,
      bool materialize_tuple, MemPool* pool, Tuple* tuple) const;

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  Status ProcessFooter() WARN_UNUSED_RESULT;

  /// Populates 'column_readers' for the slots in 'tuple_desc', including creating child
  /// readers for any collections. Schema resolution is handled in this function as
  /// well. Fills in the appropriate template tuple slot with NULL for any materialized
  /// fields missing in the file.
  Status CreateColumnReaders(const TupleDescriptor& tuple_desc,
      const ParquetSchemaResolver& schema_resolver,
      std::vector<ParquetColumnReader*>* column_readers) WARN_UNUSED_RESULT;

  /// Returns the total number of scalar column readers in 'column_readers', including
  /// the children of collection readers.
  int CountScalarColumns(const std::vector<ParquetColumnReader*>& column_readers);

  /// Creates a column reader that reads one value for each item in the table or
  /// collection element corresponding to 'parent_path'. 'parent_path' should point to
  /// either a collection element or the root schema (i.e. empty path). The returned
  /// reader has no slot desc associated with it, meaning only NextLevels() and not
  /// ReadValue() can be called on it.
  ///
  /// This is used for counting item values, rather than materializing any values. For
  /// example, in a count(*) over a collection, there are no values to materialize, but we
  /// still need to iterate over every item in the collection to count them.
  Status CreateCountingReader(const SchemaPath& parent_path,
      const ParquetSchemaResolver& schema_resolver,
      ParquetColumnReader** reader)
      WARN_UNUSED_RESULT;

  /// Walks file_metadata_ and initiates reading the materialized columns.  This
  /// initializes 'scalar_readers_' and divides reservation between the columns but
  /// does not start any scan ranges.
  Status InitScalarColumns() WARN_UNUSED_RESULT;

  /// Decides how to divide stream_->reservation() between the columns. May increase
  /// the reservation if more reservation would enable more efficient I/O for the
  /// current columns being scanned. Sets the reservation on each corresponding reader
  /// in 'column_readers'.
  Status DivideReservationBetweenColumns(
      const std::vector<BaseScalarColumnReader*>& column_readers);

  /// Compute the ideal reservation to scan a file with scan range lengths
  /// 'col_range_lengths' given the min and max buffer size of the singleton DiskIoMgr
  /// in ExecEnv.
  static int64_t ComputeIdealReservation(const std::vector<int64_t>& col_range_lengths);

  /// Helper for DivideReservationBetweenColumns(). Implements the core algorithm for
  /// dividing a reservation of 'reservation_to_distribute' bytes between columns with
  /// scan range lengths 'col_range_lengths' given a min and max buffer size. Returns
  /// a vector with an entry per column with the index into 'col_range_lengths' and the
  /// amount of reservation in bytes to give to that column.
  static std::vector<std::pair<int, int64_t>> DivideReservationBetweenColumnsHelper(
      int64_t min_buffer_size, int64_t max_buffer_size,
      const std::vector<int64_t>& col_range_lengths, int64_t reservation_to_distribute);

  /// Initializes the column readers in collection_readers_.
  void InitCollectionColumns();

  /// Initialize dictionaries for all column readers
  Status InitDictionaries(const std::vector<BaseScalarColumnReader*>& column_readers)
      WARN_UNUSED_RESULT;

  /// Performs some validation once we've reached the end of a row group to help detect
  /// bugs or bad input files.
  Status ValidateEndOfRowGroup(const std::vector<ParquetColumnReader*>& column_readers,
      int row_group_idx, int64_t rows_read) WARN_UNUSED_RESULT;

  /// Part of the HdfsScanner interface, not used in Parquet.
  Status InitNewRange() WARN_UNUSED_RESULT { return Status::OK(); }

  /// Transfers the remaining resources backing tuples such as IO buffers and memory
  /// from mem pools to the given row batch. Closes all column readers.
  /// Should be called after completing a row group and when returning the last batch.
  void FlushRowGroupResources(RowBatch* row_batch);

  /// Releases resources associated with a row group that was skipped and closes all
  /// column readers. Should be called after skipping a row group from which no rows
  /// were returned.
  void ReleaseSkippedRowGroupResources();

  /// Evaluates whether the column reader is eligible for dictionary predicates
  bool IsDictFilterable(ParquetColumnReader* col_reader);

  /// Evaluates whether the column reader is eligible for dictionary predicates.
  bool IsDictFilterable(BaseScalarColumnReader* col_reader);

  /// Partitions the readers into scalar and collection readers. The collection readers
  /// are flattened into collection_readers_. The scalar readers are partitioned into
  /// dict_filterable_readers_ and non_dict_filterable_readers_ depending on whether
  /// dictionary filtering is enabled and the reader can be dictionary filtered. All
  /// scalar readers are also flattened into scalar_readers_.
  void PartitionReaders(const vector<ParquetColumnReader*>& readers,
                        bool can_eval_dict_filters);

  /// Divides the column readers into dict_filterable_readers_,
  /// non_dict_filterable_readers_ and collection_readers_. Allocates memory for
  /// dict_filter_tuple_map_.
  Status InitDictFilterStructures() WARN_UNUSED_RESULT;

  /// Returns true if all of the data pages in the column chunk are dictionary encoded
  bool IsDictionaryEncoded(const parquet::ColumnMetaData& col_metadata);

  /// Checks to see if this row group can be eliminated based on applying conjuncts
  /// to the dictionary values. Specifically, if any dictionary-encoded column has
  /// no values that pass the relevant conjuncts, then the row group can be skipped.
  Status EvalDictionaryFilters(const parquet::RowGroup& row_group,
      bool* skip_row_group) WARN_UNUSED_RESULT;

  /// Updates the counter parquet_compressed_page_size_counter_ with the given compressed
  /// page size. Called by ParquetColumnReader for each page read.
  void UpdateCompressedPageSizeCounter(int64_t compressed_page_size);

  /// Updates the counter parquet_uncompressed_page_size_counter_ with the given
  /// uncompressed page size. Called by ParquetColumnReader for each page read.
  void UpdateUncompressedPageSizeCounter(int64_t uncompressed_page_size);
};

} // namespace impala

#endif

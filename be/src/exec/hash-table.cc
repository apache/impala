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

#include "exec/hash-table.inline.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;

DEFINE_bool(enable_quadratic_probing, false, "Enable quadratic probing hash table");

const char* HashTableCtx::LLVM_CLASS_NAME = "class.impala::HashTableCtx";

// Page sizes used only for BE test. For non-testing, we use the io buffer size.
static const int64_t TEST_PAGE_SIZE = 8 * 1024 * 1024;

// Random primes to multiply the seed with.
static uint32_t SEED_PRIMES[] = {
  1, // First seed must be 1, level 0 is used by other operators in the fragment.
  1431655781,
  1183186591,
  622729787,
  472882027,
  338294347,
  275604541,
  41161739,
  29999999,
  27475109,
  611603,
  16313357,
  11380003,
  21261403,
  33393119,
  101,
  71043403
};

// Put a non-zero constant in the result location for NULL.
// We don't want(NULL, 1) to hash to the same as (0, 1).
// This needs to be as big as the biggest primitive type since the bytes
// get copied directly.
// TODO find a better approach, since primitives like CHAR(N) can be up to 128 bytes
static int64_t NULL_VALUE[] = { HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED };

// The first NUM_SMALL_BLOCKS of nodes_ are made of blocks less than the IO size (of 8MB)
// to reduce the memory footprint of small queries. In particular, we always first use a
// 64KB and a 512KB block before starting using IO-sized blocks.
static const int64_t INITIAL_DATA_PAGE_SIZES[] = { 64 * 1024, 512 * 1024 };
static const int NUM_SMALL_DATA_PAGES = sizeof(INITIAL_DATA_PAGE_SIZES) / sizeof(int64_t);

HashTableCtx::HashTableCtx(const vector<ExprContext*>& build_expr_ctxs,
    const vector<ExprContext*>& probe_expr_ctxs, bool stores_nulls, bool finds_nulls,
    int32_t initial_seed, int max_levels, int num_build_tuples)
    : build_expr_ctxs_(build_expr_ctxs),
      probe_expr_ctxs_(probe_expr_ctxs),
      stores_nulls_(stores_nulls),
      finds_nulls_(finds_nulls),
      level_(0),
      row_(reinterpret_cast<TupleRow*>(malloc(sizeof(Tuple*) * num_build_tuples))) {
  // Compute the layout and buffer size to store the evaluated expr results
  DCHECK_EQ(build_expr_ctxs_.size(), probe_expr_ctxs_.size());
  DCHECK(!build_expr_ctxs_.empty());
  results_buffer_size_ = Expr::ComputeResultsLayout(build_expr_ctxs_,
      &expr_values_buffer_offsets_, &var_result_begin_);
  expr_values_buffer_ = new uint8_t[results_buffer_size_];
  memset(expr_values_buffer_, 0, sizeof(uint8_t) * results_buffer_size_);
  expr_value_null_bits_ = new uint8_t[build_expr_ctxs.size()];

  // Populate the seeds to use for all the levels. TODO: revisit how we generate these.
  DCHECK_GE(max_levels, 0);
  DCHECK_LT(max_levels, sizeof(SEED_PRIMES) / sizeof(SEED_PRIMES[0]));
  DCHECK_NE(initial_seed, 0);
  seeds_.resize(max_levels + 1);
  seeds_[0] = initial_seed;
  for (int i = 1; i <= max_levels; ++i) {
    seeds_[i] = seeds_[i - 1] * SEED_PRIMES[i];
  }
}

void HashTableCtx::Close() {
  // TODO: use tr1::array?
  DCHECK(expr_values_buffer_ != NULL);
  delete[] expr_values_buffer_;
  expr_values_buffer_ = NULL;
  DCHECK(expr_value_null_bits_ != NULL);
  delete[] expr_value_null_bits_;
  expr_value_null_bits_ = NULL;
  free(row_);
  row_ = NULL;
}

bool HashTableCtx::EvalRow(TupleRow* row, const vector<ExprContext*>& ctxs) {
  bool has_null = false;
  for (int i = 0; i < ctxs.size(); ++i) {
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    void* val = ctxs[i]->GetValue(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;

      expr_value_null_bits_[i] = true;
      val = reinterpret_cast<void*>(&NULL_VALUE);
      has_null = true;
    } else {
      expr_value_null_bits_[i] = false;
    }
    DCHECK_LE(build_expr_ctxs_[i]->root()->type().GetSlotSize(),
              sizeof(NULL_VALUE));
    RawValue::Write(val, loc, build_expr_ctxs_[i]->root()->type(), NULL);
  }
  return has_null;
}

uint32_t HashTableCtx::HashVariableLenRow() {
  uint32_t hash = seeds_[level_];
  // Hash the non-var length portions (if there are any)
  if (var_result_begin_ != 0) {
    hash = Hash(expr_values_buffer_, var_result_begin_, hash);
  }

  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // non-string and null slots are already part of expr_values_buffer
    if (build_expr_ctxs_[i]->root()->type().type != TYPE_STRING &&
        build_expr_ctxs_[i]->root()->type().type != TYPE_VARCHAR) continue;

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (expr_value_null_bits_[i]) {
      // Hash the null random seed values at 'loc'
      hash = Hash(loc, sizeof(StringValue), hash);
    } else {
      // Hash the string
      // TODO: when using CRC hash on empty string, this only swaps bytes.
      StringValue* str = reinterpret_cast<StringValue*>(loc);
      hash = Hash(str->ptr, str->len, hash);
    }
  }
  return hash;
}

bool HashTableCtx::Equals(TupleRow* build_row) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    void* val = build_expr_ctxs_[i]->GetValue(build_row);
    if (val == NULL) {
      if (!stores_nulls_) return false;
      if (!expr_value_null_bits_[i]) return false;
      continue;
    } else {
      if (expr_value_null_bits_[i]) return false;
    }

    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    if (!RawValue::Eq(loc, val, build_expr_ctxs_[i]->root()->type())) {
      return false;
    }
  }
  return true;
}

const double HashTable::MAX_FILL_FACTOR = 0.75f;

HashTable::HashTable(RuntimeState* state, BufferedBlockMgr::Client* client,
    int num_build_tuples, BufferedTupleStream* stream, int64_t max_num_buckets,
    int64_t num_buckets)
  : state_(state),
    block_mgr_client_(client),
    tuple_stream_(stream),
    data_page_pool_(NULL),
    stores_tuples_(num_build_tuples == 1),
    quadratic_probing_(FLAGS_enable_quadratic_probing),
    total_data_page_size_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    num_duplicate_nodes_(0),
    max_num_buckets_(max_num_buckets),
    buckets_(NULL),
    num_buckets_(num_buckets),
    num_filled_buckets_(0),
    num_buckets_with_duplicates_(0),
    num_build_tuples_(num_build_tuples),
    has_matches_(false),
    num_probes_(0), num_failed_probes_(0), travel_length_(0), num_hash_collisions_(0),
    num_resizes_(0) {
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
  DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
  DCHECK(stores_tuples_ || stream != NULL);
}

HashTable::HashTable(MemPool* pool, bool quadratic_probing, int num_buckets)
  : state_(NULL),
    block_mgr_client_(NULL),
    tuple_stream_(NULL),
    data_page_pool_(pool),
    stores_tuples_(true),
    quadratic_probing_(quadratic_probing),
    total_data_page_size_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    num_duplicate_nodes_(0),
    max_num_buckets_(-1),
    buckets_(NULL),
    num_buckets_(num_buckets),
    num_filled_buckets_(0),
    num_buckets_with_duplicates_(0),
    num_build_tuples_(1),
    has_matches_(false),
    num_probes_(0), num_failed_probes_(0), travel_length_(0), num_hash_collisions_(0),
    num_resizes_(0) {
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
  DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
  bool ret = Init();
  DCHECK(ret);
}

bool HashTable::Init() {
  int64_t buckets_byte_size = num_buckets_ * sizeof(Bucket);
  if (block_mgr_client_ != NULL &&
      !state_->block_mgr()->ConsumeMemory(block_mgr_client_, buckets_byte_size)) {
    num_buckets_ = 0;
    return false;
  }
  buckets_ = reinterpret_cast<Bucket*>(malloc(buckets_byte_size));
  memset(buckets_, 0, buckets_byte_size);
  return GrowNodeArray();
}

void HashTable::Close() {
  // Print statistics only for the large or heavily used hash tables.
  // TODO: Tweak these numbers/conditions, or print them always?
  const int64_t LARGE_HT = 128 * 1024;
  const int64_t HEAVILY_USED = 1024 * 1024;
  // TODO: These statistics should go to the runtime profile as well.
  if ((num_buckets_ > LARGE_HT) || (num_probes_ > HEAVILY_USED)) VLOG(2) << PrintStats();
  for (int i = 0; i < data_pages_.size(); ++i) {
    data_pages_[i]->Delete();
  }
  if (ImpaladMetrics::HASH_TABLE_TOTAL_BYTES != NULL) {
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(-total_data_page_size_);
  }
  data_pages_.clear();
  if (buckets_ != NULL) free(buckets_);
  if (block_mgr_client_ != NULL) {
    state_->block_mgr()->ReleaseMemory(block_mgr_client_,
        num_buckets_ * sizeof(Bucket));
  }
}

int64_t HashTable::CurrentMemSize() const {
  return num_buckets_ * sizeof(Bucket) + num_duplicate_nodes_ * sizeof(DuplicateNode);
}

bool HashTable::CheckAndResize(uint64_t buckets_to_fill, HashTableCtx* ht_ctx) {
  uint64_t shift = 0;
  while (num_filled_buckets_ + buckets_to_fill >
         (num_buckets_ << shift) * MAX_FILL_FACTOR) {
    // TODO: next prime instead of double?
    ++shift;
  }
  if (shift > 0) return ResizeBuckets(num_buckets_ << shift, ht_ctx);
  return true;
}

bool HashTable::ResizeBuckets(int64_t num_buckets, HashTableCtx* ht_ctx) {
  DCHECK_EQ((num_buckets & (num_buckets-1)), 0)
      << "num_buckets=" << num_buckets << " must be a power of 2";
  DCHECK_GT(num_buckets, num_filled_buckets_) << "Cannot shrink the hash table to "
      "smaller number of buckets than the number of filled buckets.";
  VLOG(2) << "Resizing hash table from "
          << num_buckets_ << " to " << num_buckets << " buckets.";
  if (max_num_buckets_ != -1 && num_buckets > max_num_buckets_) return false;
  ++num_resizes_;

  // All memory that can grow proportional to the input should come from the block mgrs
  // mem tracker.
  // Note that while we copying over the contents of the old hash table, we need to have
  // allocated both the old and the new hash table. Once we finish, we return the memory
  // of the old hash table.
  int64_t old_size = num_buckets_ * sizeof(Bucket);
  int64_t new_size = num_buckets * sizeof(Bucket);
  if (block_mgr_client_ != NULL &&
      !state_->block_mgr()->ConsumeMemory(block_mgr_client_, new_size)) {
    return false;
  }
  Bucket* new_buckets = reinterpret_cast<Bucket*>(malloc(new_size));
  DCHECK(new_buckets != NULL);
  memset(new_buckets, 0, new_size);

  // Walk the old table and copy all the filled buckets to the new (resized) table.
  // We do not have to do anything with the duplicate nodes. This operation is expected
  // to succeed.
  for (HashTable::Iterator iter = Begin(ht_ctx); !iter.AtEnd();
       NextFilledBucket(&iter.bucket_idx_, &iter.node_)) {
    Bucket* bucket_to_copy = &buckets_[iter.bucket_idx_];
    bool found = false;
    int64_t bucket_idx = Probe(new_buckets, num_buckets, NULL, bucket_to_copy->hash,
                               &found);
    DCHECK(!found);
    DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND) << " Probe failed even though "
        " there are free buckets. " << num_buckets << " " << num_filled_buckets_;
    Bucket* dst_bucket = &new_buckets[bucket_idx];
    *dst_bucket = *bucket_to_copy;
  }

  num_buckets_ = num_buckets;
  free(buckets_);
  buckets_ = new_buckets;
  // TODO: Remove this check, i.e. block_mgr_client_ should always be != NULL,
  // see IMPALA-1656.
  if (block_mgr_client_ != NULL) {
    state_->block_mgr()->ReleaseMemory(block_mgr_client_, old_size);
  }
  return true;
}

bool HashTable::GrowNodeArray() {
  int64_t page_size = 0;
  if (block_mgr_client_ != NULL) {
    page_size = state_->block_mgr()->max_block_size();;
    if (data_pages_.size() < NUM_SMALL_DATA_PAGES) {
      page_size = min(page_size, INITIAL_DATA_PAGE_SIZES[data_pages_.size()]);
    }
    BufferedBlockMgr::Block* block = NULL;
    Status status = state_->block_mgr()->GetNewBlock(
        block_mgr_client_, NULL, &block, page_size);
    DCHECK(status.ok() || block == NULL);
    if (block == NULL) return false;
    data_pages_.push_back(block);
    next_node_ = block->Allocate<DuplicateNode>(page_size);
    ImpaladMetrics::HASH_TABLE_TOTAL_BYTES->Increment(page_size);
  } else {
    // Only used for testing.
    DCHECK(data_page_pool_ != NULL);
    page_size = TEST_PAGE_SIZE;
    next_node_ = reinterpret_cast<DuplicateNode*>(data_page_pool_->Allocate(page_size));
    if (data_page_pool_->mem_tracker()->LimitExceeded()) return false;
    DCHECK(next_node_ != NULL);
  }
  node_remaining_current_page_ = page_size / sizeof(DuplicateNode);
  total_data_page_size_ += page_size;
  return true;
}

void HashTable::DebugStringTuple(stringstream& ss, HtData& htdata,
    const RowDescriptor* desc) {
  if (stores_tuples_) {
    ss << "(" << htdata.tuple << ")";
  } else {
    ss << "(" << htdata.idx.block() << ", " << htdata.idx.idx()
       << ", " << htdata.idx.offset() << ")";
  }
  if (desc != NULL) {
    Tuple* row[num_build_tuples_];
    ss << " " << PrintRow(GetRow(htdata, reinterpret_cast<TupleRow*>(row)), *desc);
  }
}

string HashTable::DebugString(bool skip_empty, bool show_match,
    const RowDescriptor* desc) {
  stringstream ss;
  ss << endl;
  for (int i = 0; i < num_buckets_; ++i) {
    if (skip_empty && !buckets_[i].filled) continue;
    ss << i << ": ";
    if (show_match) {
      if (buckets_[i].matched) {
        ss << " [M]";
      } else {
        ss << " [U]";
      }
    }
    if (buckets_[i].hasDuplicates) {
      DuplicateNode* node = buckets_[i].bucketData.duplicates;
      bool first = true;
      ss << " [D] ";
      while (node != NULL) {
        if (!first) ss << ",";
        DebugStringTuple(ss, node->htdata, desc);
        node = node->next;
        first = false;
      }
    } else {
      ss << " [B] ";
      if (buckets_[i].filled) {
        DebugStringTuple(ss, buckets_[i].bucketData.htdata, desc);
      } else {
        ss << " - ";
      }
    }
    ss << endl;
  }
  return ss.str();
}

string HashTable::PrintStats() const {
  double curr_fill_factor = (double)num_filled_buckets_/(double)num_buckets_;
  double avg_travel = (double)travel_length_/(double)num_probes_;
  double avg_collisions = (double)num_hash_collisions_/(double)num_filled_buckets_;
  stringstream ss;
  ss << "Buckets: " << num_buckets_ << " " << num_filled_buckets_ << " "
     << curr_fill_factor << endl;
  ss << "Duplicates: " << num_buckets_with_duplicates_ << " buckets "
     << num_duplicate_nodes_ << " nodes" << endl;
  ss << "Probes: " << num_probes_ << endl;
  ss << "FailedProbes: " << num_failed_probes_ << endl;
  ss << "Travel: " << travel_length_ << " " << avg_travel << endl;
  ss << "HashCollisions: " << num_hash_collisions_ << " " << avg_collisions << endl;
  ss << "Resizes: " << num_resizes_ << endl;
  return ss.str();
}

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void CodegenAssignNullValue(LlvmCodeGen* codegen,
    LlvmCodeGen::LlvmBuilder* builder, Value* dst, const ColumnType& type) {
  int64_t fvn_seed = HashUtil::FNV_SEED;

  if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR) {
    Value* dst_ptr = builder->CreateStructGEP(dst, 0, "string_ptr");
    Value* dst_len = builder->CreateStructGEP(dst, 1, "string_len");
    Value* null_len = codegen->GetIntConstant(TYPE_INT, fvn_seed);
    Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
    builder->CreateStore(null_ptr, dst_ptr);
    builder->CreateStore(null_len, dst_len);
  } else {
    Value* null_value = NULL;
    // Get a type specific representation of fvn_seed
    switch (type.type) {
      case TYPE_BOOLEAN:
        // In results, booleans are stored as 1 byte
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetIntConstant(TYPE_TINYINT, fvn_seed);
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        null_value = codegen->GetIntConstant(type.type, fvn_seed);
        break;
      case TYPE_FLOAT: {
        // Don't care about the value, just the bit pattern
        float fvn_seed_float = *reinterpret_cast<float*>(&fvn_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_float));
        break;
      }
      case TYPE_DOUBLE: {
        // Don't care about the value, just the bit pattern
        double fvn_seed_double = *reinterpret_cast<double*>(&fvn_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_double));
        break;
      }
      default:
        DCHECK(false);
    }
    builder->CreateStore(null_value, dst);
  }
}

// Codegen for evaluating a tuple row over either build_expr_ctxs_ or probe_expr_ctxs_.
// For the case where we are joining on a single int, the IR looks like
// define i1 @EvalBuildRow(%"class.impala::HashTableCtx"* %this_ptr,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %result = call i64 @GetSlotRef1(%"class.impala::ExprContext"* inttoptr
//                                     (i64 67971664 to %"class.impala::ExprContext"*),
//                                   %"class.impala::TupleRow"* %row)
//   %is_null = trunc i64 %result to i1
//   %0 = zext i1 %is_null to i8
//   store i8 %0, i8* inttoptr (i64 95753144 to i8*)
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   store i32 -2128831035, i32* inttoptr (i64 95753128 to i32*)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %1 = ashr i64 %result, 32
//   %2 = trunc i64 %1 to i32
//   store i32 %2, i32* inttoptr (i64 95753128 to i32*)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   ret i1 true
// }
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Function* HashTableCtx::CodegenEvalRow(RuntimeState* state, bool build) {
  // TODO: CodegenAssignNullValue() can't handle TYPE_TIMESTAMP or TYPE_DECIMAL yet
  const vector<ExprContext*>& ctxs = build ? build_expr_ctxs_ : probe_expr_ctxs_;
  for (int i = 0; i < ctxs.size(); ++i) {
    PrimitiveType type = ctxs[i]->root()->type().type;
    if (type == TYPE_TIMESTAMP || type == TYPE_DECIMAL || type == TYPE_CHAR) return NULL;
  }

  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;

  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(HashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvalBuildRow" : "EvalProbeRow",
      codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);

  Value* row = args[1];
  Value* has_null = codegen->false_value();

  for (int i = 0; i < ctxs.size(); ++i) {
    // TODO: refactor this to somewhere else?  This is not hash table specific except for
    // the null handling bit and would be used for anyone that needs to materialize a
    // vector of exprs
    // Convert result buffer to llvm ptr type
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    Value* llvm_loc = codegen->CastPtrToLlvmPtr(
        codegen->GetPtrType(ctxs[i]->root()->type()), loc);

    BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
    BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
    BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

    // Call expr
    Function* expr_fn;
    Status status = ctxs[i]->root()->GetCodegendComputeFn(state, &expr_fn);
    if (!status.ok()) {
      VLOG_QUERY << "Problem with CodegenEvalRow: " << status.GetDetail();
      fn->eraseFromParent(); // deletes function
      return NULL;
    }

    Value* ctx_arg = codegen->CastPtrToLlvmPtr(
        codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME), ctxs[i]);
    Value* expr_fn_args[] = { ctx_arg, row };
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
        codegen, &builder, ctxs[i]->root()->type(), expr_fn, expr_fn_args, "result");
    Value* is_null = result.GetIsNull();

    // Set null-byte result
    Value* null_byte = builder.CreateZExt(is_null, codegen->GetType(TYPE_TINYINT));
    uint8_t* null_byte_loc = &expr_value_null_bits_[i];
    Value* llvm_null_byte_loc =
        codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
    builder.CreateStore(null_byte, llvm_null_byte_loc);

    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    if (!stores_nulls_) {
      // hash table doesn't store nulls, no reason to keep evaluating exprs
      builder.CreateRet(codegen->true_value());
    } else {
      CodegenAssignNullValue(codegen, &builder, llvm_loc, ctxs[i]->root()->type());
      builder.CreateBr(continue_block);
    }

    // Not null block
    builder.SetInsertPoint(not_null_block);
    result.ToNativePtr(llvm_loc);
    builder.CreateBr(continue_block);

    // Continue block
    builder.SetInsertPoint(continue_block);
    if (stores_nulls_) {
      // Update has_null
      PHINode* is_null_phi = builder.CreatePHI(codegen->boolean_type(), 2, "is_null_phi");
      is_null_phi->addIncoming(codegen->true_value(), null_block);
      is_null_phi->addIncoming(codegen->false_value(), not_null_block);
      has_null = builder.CreateOr(has_null, is_null_phi, "has_null");
    }
  }
  builder.CreateRet(has_null);

  return codegen->FinalizeFunction(fn);
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
// define i32 @HashCurrentRow(%"class.impala::HashTableCtx"* %this_ptr) #20 {
// entry:
//   %seed = call i32 @GetHashSeed(%"class.impala::HashTableCtx"* %this_ptr)
//   %0 = call i32 @CrcHash16(i8* inttoptr (i64 119151296 to i8*), i32 16, i32 %seed)
//   %1 = load i8* inttoptr (i64 119943721 to i8*)
//   %2 = icmp ne i8 %1, 0
//   br i1 %2, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %3 = call i32 @CrcHash161(i8* inttoptr (i64 119151312 to i8*), i32 16, i32 %0)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %4 = load i8** getelementptr inbounds (%"struct.impala::StringValue"* inttoptr
//       (i64 119151312 to %"struct.impala::StringValue"*), i32 0, i32 0)
//   %5 = load i32* getelementptr inbounds (%"struct.impala::StringValue"* inttoptr
//       (i64 119151312 to %"struct.impala::StringValue"*), i32 0, i32 1)
//   %6 = call i32 @IrCrcHash(i8* %4, i32 %5, i32 %0)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %7 = phi i32 [ %6, %not_null ], [ %3, %null ]
//   call void @set_hash(%"class.impala::HashTableCtx"* %this_ptr, i32 %7)
//   ret i32 %7
// }
Function* HashTableCtx::CodegenHashCurrentRow(RuntimeState* state, bool use_murmur) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_expr_ctxs_[i]->root()->type().type == TYPE_CHAR) return NULL;
  }

  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(HashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen,
      (use_murmur ? "MurmurHashCurrentRow" : "HashCurrentRow"),
      codegen->GetType(TYPE_INT));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* this_arg;
  Function* fn = prototype.GeneratePrototype(&builder, &this_arg);

  // Call GetHashSeed() to get seeds_[level_]
  Function* get_hash_seed_fn = codegen->GetFunction(IRFunction::HASH_TABLE_GET_HASH_SEED);
  Value* seed = builder.CreateCall(get_hash_seed_fn, this_arg, "seed");

  Value* hash_result = seed;
  Value* data = codegen->CastPtrToLlvmPtr(codegen->ptr_type(), expr_values_buffer_);
  if (var_result_begin_ == -1) {
    // No variable length slots, just hash what is in 'expr_values_buffer_'
    if (results_buffer_size_ > 0) {
      Function* hash_fn = use_murmur ?
                          codegen->GetMurmurHashFunction(results_buffer_size_) :
                          codegen->GetHashFunction(results_buffer_size_);
      Value* len = codegen->GetIntConstant(TYPE_INT, results_buffer_size_);
      hash_result = builder.CreateCall3(hash_fn, data, len, hash_result, "hash");
    }
  } else {
    if (var_result_begin_ > 0) {
      Function* hash_fn = use_murmur ?
                          codegen->GetMurmurHashFunction(var_result_begin_) :
                          codegen->GetHashFunction(var_result_begin_);
      Value* len = codegen->GetIntConstant(TYPE_INT, var_result_begin_);
      hash_result = builder.CreateCall3(hash_fn, data, len, hash_result, "hash");
    }

    // Hash string slots
    for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
      if (build_expr_ctxs_[i]->root()->type().type != TYPE_STRING
          && build_expr_ctxs_[i]->root()->type().type != TYPE_VARCHAR) continue;

      BasicBlock* null_block = NULL;
      BasicBlock* not_null_block = NULL;
      BasicBlock* continue_block = NULL;
      Value* str_null_result = NULL;

      void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];

      // If the hash table stores nulls, we need to check if the stringval
      // evaluated to NULL
      if (stores_nulls_) {
        null_block = BasicBlock::Create(context, "null", fn);
        not_null_block = BasicBlock::Create(context, "not_null", fn);
        continue_block = BasicBlock::Create(context, "continue", fn);

        uint8_t* null_byte_loc = &expr_value_null_bits_[i];
        Value* llvm_null_byte_loc =
            codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
        Value* null_byte = builder.CreateLoad(llvm_null_byte_loc, "null_byte");
        Value* is_null = builder.CreateICmpNE(null_byte,
            codegen->GetIntConstant(TYPE_TINYINT, 0), "is_null");
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // For null, we just want to call the hash function on the portion of
        // the data
        builder.SetInsertPoint(null_block);
        Function* null_hash_fn = use_murmur ?
                                 codegen->GetMurmurHashFunction(sizeof(StringValue)) :
                                 codegen->GetHashFunction(sizeof(StringValue));
        Value* llvm_loc = codegen->CastPtrToLlvmPtr(codegen->ptr_type(), loc);
        Value* len = codegen->GetIntConstant(TYPE_INT, sizeof(StringValue));
        str_null_result =
            builder.CreateCall3(null_hash_fn, llvm_loc, len, hash_result, "str_null");
        builder.CreateBr(continue_block);

        builder.SetInsertPoint(not_null_block);
      }

      // Convert expr_values_buffer_ loc to llvm value
      Value* str_val = codegen->CastPtrToLlvmPtr(codegen->GetPtrType(TYPE_STRING), loc);

      Value* ptr = builder.CreateStructGEP(str_val, 0);
      Value* len = builder.CreateStructGEP(str_val, 1);
      ptr = builder.CreateLoad(ptr, "ptr");
      len = builder.CreateLoad(len, "len");

      // Call hash(ptr, len, hash_result);
      Function* general_hash_fn = use_murmur ? codegen->GetMurmurHashFunction() :
                                  codegen->GetHashFunction();
      Value* string_hash_result =
          builder.CreateCall3(general_hash_fn, ptr, len, hash_result, "string_hash");

      if (stores_nulls_) {
        builder.CreateBr(continue_block);
        builder.SetInsertPoint(continue_block);
        // Use phi node to reconcile that we could have come from the string-null
        // path and string not null paths.
        PHINode* phi_node = builder.CreatePHI(codegen->GetType(TYPE_INT), 2, "hash_phi");
        phi_node->addIncoming(string_hash_result, not_null_block);
        phi_node->addIncoming(str_null_result, null_block);
        hash_result = phi_node;
      } else {
        hash_result = string_hash_result;
      }
    }
  }

  builder.CreateRet(hash_result);
  return codegen->FinalizeFunction(fn);
}

// Codegen for HashTableCtx::Equals.  For a hash table with two exprs (string,int),
// the IR looks like:
//
// define i1 @Equals(%"class.impala::HashTableCtx"* %this_ptr,
//                   %"class.impala::TupleRow"* %row) {
// entry:
//   %result = call i64 @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//                                  (i64 146381856 to %"class.impala::ExprContext"*),
//                                  %"class.impala::TupleRow"* %row)
//   %0 = trunc i64 %result to i1
//   br i1 %0, label %null, label %not_null
//
// false_block:                            ; preds = %not_null2, %null1, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 false, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   %1 = load i32* inttoptr (i64 104774368 to i32*)
//   %2 = ashr i64 %result, 32
//   %3 = trunc i64 %2 to i32
//   %cmp_raw = icmp eq i32 %3, %1
//   br i1 %cmp_raw, label %continue, label %false_block
//
// continue:                                         ; preds = %not_null, %null
//   %result4 = call { i64, i8* } @GetSlotRef1(
//       %"class.impala::ExprContext"* inttoptr
//       (i64 146381696 to %"class.impala::ExprContext"*),
//       %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %5 = trunc i64 %4 to i1
//   br i1 %5, label %null1, label %not_null2
//
// null1:                                            ; preds = %continue
//   br i1 false, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   %6 = extractvalue { i64, i8* } %result4, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %result5 = extractvalue { i64, i8* } %result4, 1
//   %cmp_raw6 = call i1 @_Z11StringValEQPciPKN6impala11StringValueE(
//       i8* %result5, i32 %8, %"struct.impala::StringValue"* inttoptr
//       (i64 104774384 to %"struct.impala::StringValue"*))
//   br i1 %cmp_raw6, label %continue3, label %false_block
//
// continue3:                                        ; preds = %not_null2, %null1
//   ret i1 true
// }
Function* HashTableCtx::CodegenEquals(RuntimeState* state) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_expr_ctxs_[i]->root()->type().type == TYPE_CHAR) return NULL;
  }

  LlvmCodeGen* codegen;
  if (!state->GetCodegen(&codegen).ok()) return NULL;
  // Get types to generate function prototype
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Type* this_type = codegen->GetType(HashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = PointerType::get(this_type, 0);

  LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  Value* row = args[1];

  BasicBlock* false_block = BasicBlock::Create(context, "false_block", fn);
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
    BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
    BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

    // call GetValue on build_exprs[i]
    Function* expr_fn;
    Status status = build_expr_ctxs_[i]->root()->GetCodegendComputeFn(state, &expr_fn);
    if (!status.ok()) {
      VLOG_QUERY << "Problem with CodegenEquals: " << status.GetDetail();
      fn->eraseFromParent(); // deletes function
      return NULL;
    }

    Value* ctx_arg = codegen->CastPtrToLlvmPtr(
        codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME), build_expr_ctxs_[i]);
    Value* expr_fn_args[] = { ctx_arg, row };
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        build_expr_ctxs_[i]->root()->type(), expr_fn, expr_fn_args, "result");
    Value* is_null = result.GetIsNull();

    // Determine if probe is null (i.e. expr_value_null_bits_[i] == true). In
    // the case where the hash table does not store nulls, this is always false.
    Value* probe_is_null = codegen->false_value();
    uint8_t* null_byte_loc = &expr_value_null_bits_[i];
    if (stores_nulls_) {
      Value* llvm_null_byte_loc =
          codegen->CastPtrToLlvmPtr(codegen->ptr_type(), null_byte_loc);
      Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
      probe_is_null = builder.CreateICmpNE(null_byte,
                                           codegen->GetIntConstant(TYPE_TINYINT, 0));
    }

    // Get llvm value for probe_val from 'expr_values_buffer_'
    void* loc = expr_values_buffer_ + expr_values_buffer_offsets_[i];
    Value* probe_val = codegen->CastPtrToLlvmPtr(
        codegen->GetPtrType(build_expr_ctxs_[i]->root()->type()), loc);

    // Branch for GetValue() returning NULL
    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    builder.CreateCondBr(probe_is_null, continue_block, false_block);

    // Not-null block
    builder.SetInsertPoint(not_null_block);
    if (stores_nulls_) {
      BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", fn);
      // First need to compare that probe expr[i] is not null
      builder.CreateCondBr(probe_is_null, false_block, cmp_block);
      builder.SetInsertPoint(cmp_block);
    }
    // Check result == probe_val
    Value* is_equal = result.EqToNativePtr(probe_val);
    builder.CreateCondBr(is_equal, continue_block, false_block);

    builder.SetInsertPoint(continue_block);
  }
  builder.CreateRet(codegen->true_value());

  builder.SetInsertPoint(false_block);
  builder.CreateRet(codegen->false_value());

  return codegen->FinalizeFunction(fn);
}

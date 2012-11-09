#ifndef IMPALA_EXPERIMENTS_HASHING_HASH_STORE_H
#define IMPALA_EXPERIMENTS_HASHING_HASH_STORE_H


#include <glog/logging.h>
#include <vector>

#include "common/compiler-util.h"
#include "hashing-util.h"
#include "standard-hash-table.h"
#include "standard-hash-table.inline.h"
#include "tuple-types.h"

namespace impala {

// Hash-based storage structure.
// Made up of a number of L1-sized hashtables.
// There is a tree structure of buffers so that writes to each hashtable are batched.
// The fanout between each adjacent pair of levels is capped at a factor of MAX_FANOUT.
// That is, the ratio of the number of buffers in level i and the number of buffers at
// level i-1 cannot exceed MAX_FANOUT; this means that each buffer only has to fanout
// into MAX_FANOUT buffers at the next level down, which, as long as MAX_FANOUT is
// chosen well, can ensure that processing a buffer entails writing into a small enough
// working set that cache locality is good.
// We grow at the bottom, and subtrees grow independently.
// To begin, the first level is doubled up to a sizeof MAX_FANOUT.
// Then all MAX_FANOUT buffers will have 2 buffers placed under them. From that point on,
// any top-level buffer can double its children as needed. When a top-level buffer
// reaches 2^7 children, its next growth move occurs by adding 2 buffers under each
//  of its children.
//
// Templatized on the number of bytes that each buffer holds (buffer_bytes) for
// easy testing. (Buffers at all levels are currently equal size.)
// Disabling buffering is not implemented.
template <int buffer_bytes>
class HashStore {
 public:
  HashStore() {
    super_root_ = new Buffer;
    super_root_->children = new Buffer[1];
    delete super_root_->table;
    super_root_->table = NULL;
  }
  ~HashStore() {
    delete super_root_;
  }

  // Run hash aggregation on tuples, which contains n tuples.
  void Aggregate(ProbeTuple* tuples, uint64_t n) {
    for (uint64_t i = 0; i < n; ++i) {
      ProbeTuple* tuple = &tuples[i];
      tuple->hash = hash_id(tuple->id);
      WriteTuple(tuple, super_root_, 0);
    }

    ProcessEverything(super_root_, 0);
  }

 private:
  friend class GrowingTest;

  // Calculate how many tuples fit in a buffer of size buffer_bytes
  // Special case when there are no buffers because otherwise we'll define a negative-size array.
  // TODO Better way to do this compile-time?

  // Don't count the bytes needed for count and start.
  // TODO new fields
  struct Buffer;

  static const int TUPLE_BYTES = buffer_bytes == 0 ? 0 :
    buffer_bytes - 3 * sizeof(uint32_t) - sizeof(StandardHashTable*) - sizeof(Buffer*);
  static const int BUFFER_SIZE = buffer_bytes == 0 ? 0 : TUPLE_BYTES / sizeof(ProbeTuple);
  static const int PADDING_BYTES = buffer_bytes == 0 ? 0 : TUPLE_BYTES % sizeof(ProbeTuple);

  static const uint32_t ONES_MASK = 0xFFFFFFFF;
  static const int HASH_BITS = 32;
  // No level of buffering can fanout with a branching factor above 2^7.
  static const int MAX_FANOUT_BITS = 6;
  static const int MAX_FANOUT = 1<<MAX_FANOUT_BITS;
  // Can't keep partitioning on hash bits once we run out of bits.
  // Do not allow more than MAX_LEVELS levels of fanout.
  // The, the range of potentially valid parent level numbers is [0,MAX_LEVELS).
  static const int MAX_LEVELS = 4;

  // A write buffer in front of a hash table.
  struct Buffer {
    // array of deferred tuples
    ProbeTuple tuples[BUFFER_SIZE];
    // how many tuples are in the array
    uint32_t count;
    // Where to start copying from this. Nonzero when we're in the middle of processing
    // this buffer.
    uint32_t start;
    // What this is buffering.
    // If table != NULL, then this buffer is backed by a hashtable. Otherwise, it is
    // backed by 2^children_bits child buffers, pointed to by children.
    StandardHashTable* table;
    Buffer* children;
    uint32_t children_bits;
    // padding up to a cache line
    uint8_t padding[PADDING_BYTES];

    Buffer() {
      children_bits = 0;
      children = NULL;
      table = new StandardHashTable;
      count = 0;
      start = 0;
    }

    ~Buffer() {
      delete table;
      delete[] children;
    }

    bool Empty() const {
      return count == 0;
    }

    bool Full() const {
      return static_cast<int32_t>(count) >= BUFFER_SIZE;
    }

    bool BottomLevel() const {
      return table != NULL;
    }
  } __attribute__((__packed__)) __attribute__((aligned(64)));

  // Recursively ensure that all tuples buffered anywhere below parent get processed.
  // Precondition: parent is empty.
  void ProcessEverything(Buffer* parent, int level) {
    DCHECK(parent->Empty());
    if (parent->BottomLevel()) return;
    DCHECK(parent->children != NULL);
    for (int i = 0; i < 1<<parent->children_bits; ++i) {
      Buffer* child = &parent->children[i];
      while (!child->Empty()) {
        bool doubled = !ProcessBuffer(parent, child, level);
        if (doubled) {
          // this whole level has been rehashed. Start over
          i = -1;
          break;
        }
      }
    }
    for (int i = 0; i < 1<<parent->children_bits; ++i) {
      ProcessEverything(&parent->children[i], level + 1);
    }
  }

  // Returns the total number of tuples in all tables in this HashStore.
  // Used for tests.
  uint64_t TupleCount() const {
    return TupleCount(super_root_);
  }

  // Returns the total number of tuples in all tables under tree.
  uint64_t TupleCount(const Buffer* tree) const {
    uint64_t count = 0;
    if (tree->BottomLevel()) {
      StandardHashTable& ht = *tree->table;
      for (StandardHashTable::Iterator it = ht.Begin(); it.HasNext(); it.Next()) {
        count += it.GetRow()->count;
      }
    } else {
      for (int i = 0; i < 1<<tree->children_bits; ++i) {
        count += TupleCount(&tree->children[i]);
      }
    }
    return count;
  }

  // Returns the total number of tables in this HashStore
  // Used for tests.
  int TableCount() const {
    return TableCount(super_root_);
  }

  // Returns the total number of tables in all nodes under tree.
  int TableCount(const Buffer* tree) const {
    int count = 0;
    if (tree->BottomLevel()) {
      count = 1;
    }
    else {
      for (int i = 0; i < 1<<tree->children_bits; ++i) {
        count += TableCount(&tree->children[i]);
      }
    }
    return count;
  }

  // Doubles the number of tables under parent and rehashes everything in the tables.
  // This may realloc parent->children.
  // Precondition: All children of parent must be backed by tables. (We don't grow
  // a level once there are levels below its children.
  void DoubleAndRehash(Buffer* parent, int level) {
    DCHECK(parent != NULL);
    if (parent->children_bits < MAX_FANOUT_BITS) {
      // Double this level
      uint32_t old_children_bits = parent->children_bits;
      ++parent->children_bits;
      Buffer* old_children = parent->children;
      parent->children = new Buffer[1<<parent->children_bits];
      for (int b = 0; b < 1<<old_children_bits; ++b) {
        // rehash buffer b.
        Buffer* buffer = &old_children[b];
        for (int i = buffer->start; i < buffer->count; ++i) {
          // Buffer can't be full because each buffer we're writing to is only fed by one
          // buffer we're reading from, so this should always simply write to the buffer.
          BufferTuple(&buffer->tuples[i], parent, level);
        }
        // rehash table that was backing buffer b
        StandardHashTable& table = *buffer->table;
        for (StandardHashTable::Iterator it = table.Begin(); it.HasNext(); it.Next()) {
          // TODO might want to buffer these writes too. Though any one hash table will
          // only map to 2 new ones. Haven't seen this show up as noticeable in profiling.
          const BuildTuple* tuple = it.GetRow();
          PutTuple(tuple, TableFor(tuple->hash, parent, level));
        }
      }
      delete[] old_children;
    } else {
      // Add a new level under this level

      // Don't allow growing beyond MAX_LEVELS levels of fanout.
      // MAX_LEVELS-1 is the maximum legal level number.
      // This means that the last acceptable level to have a level added under it is MAX_LEVELS - 2.
      // TODO For now just failing, improve to grow the tables.
      int new_level = level + 1;
      CHECK_LT(new_level, MAX_LEVELS);

      for (int b = 0; b < 1<<parent->children_bits; ++b) {
        Buffer* child = &parent->children[b];
        DCHECK(child->children == NULL);
        DCHECK_EQ(0, child->children_bits);
        child->children_bits = 1;
        child->children = new Buffer[1<<child->children_bits];
        // rehash table that was backing child
        StandardHashTable& table = *child->table;
        for (StandardHashTable::Iterator it = table.Begin(); it.HasNext(); it.Next()) {
          // TODO might want to child these writes too. Though any one hash table will
          // only map to 2 new ones. Haven't seen this show up as noticeable in profiling.
          const BuildTuple* tuple = it.GetRow();
          PutTuple(tuple, TableFor(tuple->hash, child, level + 1));
        }
        delete child->table;
        child->table = NULL;
      }
    }
  }

  // Ensure that level is not obviously invalid.
  inline void AssertLevelValid(int level) const {
    DCHECK_GE(level, 0);
  }

  // Returns the number of bits used to index all levels above level.
  inline int BitsBeforeLevel(int level) const {
    return level * MAX_FANOUT_BITS;
  }

  // Returns the index of the table (and buffer) that hash is currently mapped to
  // when indexing into the children of parent. Parent is at level level.
  inline uint32_t IndexFor(uint32_t hash, const Buffer* parent, int level) const {
    AssertLevelValid(level);
    if (parent->children_bits == 0) {
      return 0;
    }
    // Mask off all but enough bottom bits to fully index into this level
    uint32_t mask = ONES_MASK >> (HASH_BITS - parent->children_bits);
    uint32_t shifted = hash >> BitsBeforeLevel(level);
    // Shift away the bits that are used for lower (bigger) levels
    uint32_t idx = shifted & mask;
    DCHECK_LT(idx, 1<<parent->children_bits);
    return idx;
  }

  // Returns a pointer to the table that hash is currently mapped to. (parent and level
  // are same as IndexFor)
  inline StandardHashTable* TableFor(uint32_t hash, const Buffer* parent, int level) const {
    StandardHashTable* table = parent->children[IndexFor(hash, parent, level)].table;
    DCHECK(table != NULL);
    return table;
  }

  // Attempts to insert tuple into table. If table is full, returns false
  // and makes no changes.
  inline bool PutTuple(const BuildTuple* tuple, StandardHashTable* table) {
    if (UNLIKELY(table->Full())) {
      return false;
    }
    table->Insert(tuple);
    return true;
  }

  // Attempts to aggregate probe, which is mapped to table.
  // Returns true if successful, false if table is full,
  // in which case no change has occurred.
  inline bool TryProcessTuple(const ProbeTuple* probe, StandardHashTable* table) {
    // We're doing COUNT(*). Our BuildTuples in the table hold the count, so if
    // it can be found in the table, just increment. Otherwise, have to insert.
    BuildTuple* existing = table->Find(probe);
    if (existing != NULL) {
      ++existing->count;
      // Successfully processed.
      return true;
    } else {
      BuildTuple build;
      build.id = probe->id;
      build.count = 1;
      build.hash = probe->hash;
      return PutTuple(&build, table);
    }
  }

  // Buffer tuple for writing.
  inline void BufferTuple(const ProbeTuple* tuple, Buffer* parent, int level) {
    AssertLevelValid(level);
    uint32_t idx = IndexFor(tuple->hash, parent, level);
    Buffer* buffer = &parent->children[idx];
    while (UNLIKELY(buffer->Full())) {
      bool doubled = !ProcessBuffer(parent, buffer, level);
      if (doubled) {
        idx = IndexFor(tuple->hash, parent, level);
        buffer = &parent->children[idx];
      }
    }
    DCHECK(!buffer->Full());
    buffer->tuples[buffer->count++] = *tuple;
  }

  // Write tuple to whatever is below parent. (Either parent->table or one of parent's
  // child buffers.)
  // May call DoubleAndRehash on parent.
  inline void WriteTuple(const ProbeTuple* tuple, Buffer* parent, int level) {
    bool success;
    do {
      if (parent->BottomLevel()) {
        // Try to write it to the table, Double if we fail.
        // Then we will do the whole loop over, which means that if Doubling adds
        // another level, we will buffer the tuple instead of trying to write it to a
        // table.
        success = TryProcessTuple(tuple, parent->table);
        if (UNLIKELY(!success)) {
          DoubleAndRehash(parent, level);
        }
      } else {
        // Put it in the proper buffer
        BufferTuple(tuple, parent, level);
        success = true;
      }
    } while (UNLIKELY(!success));
  }

  // Process all tuples in buffer.
  // If table is NULL, writes to the next level of buffers. Else, writes to table.
  // If this causes a call to DoubleAndRehash() on the passed in parent,
  // stops processing and returns false.
  // Else, processes whole buffer and returns true.
  inline bool ProcessBuffer(Buffer* parent, Buffer* buffer, int level) {
    while (buffer->start < buffer->count) {
      // We want to process index start, but we want start to be incremented now so that
      // if we double and thus copy everything remaining in this buffer, the one we're
      // currently processing is not copied (and thus processed twice).
      const ProbeTuple* tuple = &buffer->tuples[buffer->start];
      if (buffer->BottomLevel()) {
        // No further buffering, so try to write to table.
        bool success = TryProcessTuple(tuple, buffer->table);
        if (!success) {
          DoubleAndRehash(parent, level);
          return false;
        }
      } else {
        // write it to the next buffer down buffer.
        BufferTuple(tuple, buffer, level + 1);
      }
      // Advance to the next Tuple.
      ++buffer->start;
    }
    buffer->count = 0;
    buffer->start = 0;
    return true;
  }

  // Root buffer in the tree.
  Buffer* super_root_;
};

}

#endif

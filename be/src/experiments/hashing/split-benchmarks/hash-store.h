#ifndef IMPALA_EXPERIMENTS_HASHING_HASH_STORE_H
#define IMPALA_EXPERIMENTS_HASHING_HASH_STORE_H

#include "tuple-types.h"
#include "standard-hash-table.h"

#include <emmintrin.h>
#include <glog/logging.h>

#include "standard-hash-table.inline.h"
#include "hashing-util.h"


namespace impala {

// Hash-based storage structure.
// Made up of a number of L1-sized hashtables with a write buffer in front of each.
// Templatized on the number of bytes that each buffer holds (buffer_bytes) for
// easy testing.
// To disable buffers, pass a buffer_bytes of 0.
template <int buffer_bytes>
class HashStore {
 public:
  HashStore() {
    num_tables_ = 1;
    tables_ = new StandardHashTable[num_tables_];
    buffers_ = new Buffer[num_tables_];
    buffer_buffers_ = new BufferBuffer[num_tables_];
    // Just one hashtable, so should always go to index 0.
    table_mask_ = 0;
    shift_bits_ = 32;
  }
  ~HashStore() {
    delete[] tables_;
    delete[] buffers_;
    delete[] buffer_buffers_;
  }

  // Run hash aggregation on tuples, which contains n tuples.
  void Aggregate(ProbeTuple* tuples, uint64_t n) {
    for (uint64_t i = 0; i < n; ++i) {
      ProbeTuple* tuple = &tuples[i];
      tuple->hash = hash_id(tuple->id);
      if (buffer_bytes == 0) {
        // If there's no buffering, then always just process this tuple now.
        ProcessTuple(tuple, TableFor(tuple->hash));
      } else {
        // write it to a buffer, unless it's full
        BufferTuple(tuple);
      }
    }
    if (buffer_bytes > 0) {
      // process any still-buffered tuples
      for (int i = 0; i < num_tables_; ++i) {
        bool doubled = ProcessBuffer(&buffers_[i], &buffer_buffers_[i], &tables_[i]);
        if (doubled) {
          // Everything in buffers was rehashed, so need to start over.
          i = -1;
        }
      }
    }
  }

 private:
  friend class GrowingTest;

  // Calculate how many tuples fit in a buffer of size buffer_bytes
  // Special case when there are no buffers because otherwise we'll define a negative-size array.
  // TODO Better way to do this compile-time?

  // Don't count the bytes needed for count and start.
  static const int BUFFER_OVERHEAD_BYTES = sizeof(uint32_t) + sizeof(uint32_t);
  // Keep it from using a partial cache line
  // TODO requires that cache line size be a multiple of tuple size.
  static const int TUPLE_BYTES = buffer_bytes == 0 ? 0 :
    ((buffer_bytes - BUFFER_OVERHEAD_BYTES) / 64) * 64;
  static const int BUFFER_SIZE = buffer_bytes == 0 ? 0 : ((TUPLE_BYTES / sizeof(ProbeTuple)) / 8) * 8;
  static const int PADDING_BYTES = buffer_bytes == 0 ? 0 :
    buffer_bytes - BUFFER_OVERHEAD_BYTES - (BUFFER_SIZE * sizeof(ProbeTuple));

  static const int BUFFER_BUFFER_SIZE = 32; // 4 cache lines

  static const uint32_t ONES_MASK = 0xFFFFFFFF;

  // A write buffer in front of a hash table.
  struct Buffer {
    // array of deferred tuples
    ProbeTuple tuples[BUFFER_SIZE];
    // how many tuples are in the array
    uint32_t count;
    // Where to start copying from this. Nonzero when we're in the middle of processing
    // this buffer.
    uint32_t start;
    // padding up to a cache line
    uint8_t padding[PADDING_BYTES];

    Buffer() {
      count = 0;
      start = 0;
    }

    bool Full() {
      return count >= BUFFER_SIZE;
    }
  } __attribute__((__packed__)) __attribute__((aligned(64)));

  struct BufferBuffer {
    ProbeTuple tuples[BUFFER_BUFFER_SIZE];
    int count;
    uint8_t padding[12];

    BufferBuffer() {
      count = 0;
    }

    bool Full() {
      return count >= BUFFER_BUFFER_SIZE;
    }
  };

  // Doubles the number of tables and rehashes everything in the tables.
  // This will realloc tables_ and buffers_. Thus, it is not safe to hold a pointer
  // to a buffer or table across a call to DoubleAndRehash
  void DoubleAndRehash() {
    // double the tables
    StandardHashTable* old_tables = tables_;
    int old_num_tables = num_tables_;
    num_tables_ *= 2;
    tables_ = new StandardHashTable[num_tables_];
    // double the buffers
    Buffer* old_buffers = buffers_;
    buffers_ = new Buffer[num_tables_];
    // double the buffer buffers.
    BufferBuffer* old_buffer_buffers = buffer_buffers_;
    buffer_buffers_ = new BufferBuffer[num_tables_];

    // There are now twice as many buffers, so we need the mask to consider one extra
    // bit of the hash.
    --shift_bits_;
    table_mask_ = ONES_MASK >> shift_bits_;
    // rehash tables
    for (int i = 0; i < old_num_tables; ++i) {
      StandardHashTable& table = old_tables[i];
      for (StandardHashTable::Iterator it = table.Begin(); it.HasNext(); it.Next()) {
        // TODO might want to buffer these writes too. Though any one hash table will
        // only map to 2 new ones. Haven't seen this show up as noticeable in profiling.
        const BuildTuple* tuple = it.GetRow();
        PutTuple(tuple, TableFor(tuple->hash));
      }
    }
    // rehash buffers
    for (int b = 0; b < old_num_tables; ++b) {
      Buffer& old_buffer = old_buffers[b];
      for (int i = old_buffer.start; i < old_buffer.count; ++i) {
        const ProbeTuple* tuple = &old_buffer.tuples[i];
        BufferTuple(tuple);
      }
    }

    // rehash buffer_buffers
    for (int b = 0; b < old_num_tables; ++b) {
      BufferBuffer& old_buffer_buffer = old_buffer_buffers[b];
      for (int i = 0; i < old_buffer_buffer.count; ++i) {
        const ProbeTuple* tuple = &old_buffer_buffer.tuples[i];
        BufferTuple(tuple);
      }
    }

    delete[] old_tables;
    delete[] old_buffers;
    delete[] old_buffer_buffers;
  }

  void BufferTuple(const ProbeTuple* tuple) {
    uint32_t idx = IndexFor(tuple->hash);
    BufferBuffer& buffer_buffer = buffer_buffers_[idx];
    DCHECK(!buffer_buffer.Full());
    buffer_buffer.tuples[buffer_buffer.count++] = *tuple;
    if (buffer_buffer.Full()) {
      Buffer& buffer = buffers_[idx];
      int mod = buffer.count % BUFFER_BUFFER_SIZE;
      if (LIKELY(mod == 0)) {
        // Buffer must have enough space or we would have emptied it last time.
        DCHECK_LE(buffer.count + buffer_buffer.count, BUFFER_SIZE);
        // Do a streaming write of all of buffer_buffer.
        __m128i* buffer_write_ptr = (__m128i*)&buffer.tuples[buffer.count];
        // TODO code very dependent on size of ProbeTuple.
        DCHECK_EQ(buffer_buffer.count % 2, 0);
        for (int i = 0; i < buffer_buffer.count; i += 2) {
          __m128i content = _mm_set_epi64x(*(long long*) (buffer_buffer.tuples + i),
                                           *(long long*) (buffer_buffer.tuples + i + 1));
          _mm_stream_si128(buffer_write_ptr + i/2, content);
        }
        buffer.count += buffer_buffer.count;
        if (buffer.count % 32 != 0) {
          buffer.count -= buffer_buffer.count;
        }
        //DCHECK_EQ(buffer.count % 32, 0);
        buffer_buffer.count = 0;
        if (buffer.count + BUFFER_BUFFER_SIZE >= BUFFER_SIZE) {
          // Too full to fit another whole buffer buffer, so empty it.
          ProcessBuffer(&buffer, &buffer_buffer, &tables_[idx]);
        }
      } else {
        // The number of tuples in buffer is not a multiple of the size of a
        // BufferBuffer. This means that we may not be cache-line algined.
        // This time, do non-streaming writes to make it be aligned again,
        // leaving buffer_buffer nonempty. When it fills again, it will be safe
        // to do a streaming write of cache lines.
        // TODO since BufferBuffer is multiple cache lines big, we could do
        // streaming writes of all but one line and just make sure it's cache line
        // aligned.
        int num_to_write = BUFFER_BUFFER_SIZE - mod;
        for (int i = 0; i < num_to_write; ++i) {
          const ProbeTuple* tuple = &buffer_buffer.tuples[BUFFER_BUFFER_SIZE - i - 1];
          buffer.tuples[buffer.count + i] = *tuple;
        }
        buffer_buffer.count -= num_to_write;
        buffer.count += num_to_write;
      }
    }
  }

  // Returns the index of the table (and buffer) that hash is currently mapped to.
  inline uint32_t IndexFor(uint32_t hash) {
    // table_mask is a cached value
#ifdef NDEBUG
    if (num_tables_ > 1) DCHECK_EQ(table_mask_, ONES_MASK>>shift_bits_);
#endif
    // Mask off all but enough bottom bits to fully index into our tables.
    // (CRC bottom bits seem just as random as the top bits.
    //  TODO We may eventually care to check that for the non-native hash impl too.)
    uint32_t idx = hash & table_mask_;
    DCHECK_LT(idx, num_tables_);
    return idx;
  }

  // Returns a pointer to the table that hash is currently mapped to.
  inline StandardHashTable* TableFor(uint32_t hash) {
    return &tables_[IndexFor(hash)];
  }

  // Inserts tuple into table, or the proper table after doubling.
  // Will call DoubleAndRehash() if the table is full,
  // so same restrictions as a call to DoubleAndRehash() apply.
  // Returns true if it called DoubleAndRehash, false otherwise.
  bool PutTuple(const BuildTuple* tuple, StandardHashTable* table) {
    bool doubled = false;
    while (__builtin_expect(table->Full(), 0)) {
      DoubleAndRehash();
      table = TableFor(tuple->hash);
      doubled = true;
    }
    table->Insert(tuple);
    return doubled;
  }

  // Aggregate probe, which is mapped to table. Returns true if the call leads 
  // to a call to DoubleAndRehash() else false.
  inline bool ProcessTuple(const ProbeTuple* probe, StandardHashTable* table) {
    // We're doing COUNT(*). Our BuildTuples in the table hold the count, so if
    // it can be found in the table, just increment. Otherwise, have to insert.
    BuildTuple* existing = table->Find(probe);
    if (existing != NULL) {
      ++existing->count;
      // We didn't grow everything.
      return false;
    } else {
      BuildTuple build;
      build.id = probe->id;
      build.count = 1;
      build.hash = probe->hash;
      return PutTuple(&build, table);
    }
  }

  // Process all tuples in buffer, which is backed by table.
  // If this causes a call to DoubleAndRehash(), stops processing and returns true.
  // Else, processes whole buffer and returns false.
  inline bool ProcessBuffer(Buffer* buffer, BufferBuffer* buffer_buffer,
                            StandardHashTable* table) {
    while (buffer->start < buffer->count) {
      // We want to process index start, but we want start to be incremented now so that
      // if we double and thus copy everything remaining in this buffer, the one we're
      // currently processing is not copied (and thus processed twice).
      bool doubled = ProcessTuple(&buffer->tuples[buffer->start++], table);
      if (doubled) {
        // Everything has grown, so we want to return true to signal that we didn't not
        // empty this buffer (and that buffers that have been emptied may have had tuples
        // rehashed into them).
        // Since we didn't empty this buffer, we don't want to fall through and set
        // count to 0.
        return true;
      }
    }
    buffer->count = 0;
    buffer->start = 0;

    while (buffer_buffer->count > 0) {
      bool doubled = ProcessTuple(&buffer_buffer->tuples[--buffer_buffer->count], table);
      if (doubled) return true;
    }
    return false;
  }



  // Array of hashtables
  StandardHashTable* tables_;

  // number of tables there currently are
  int num_tables_;

  // Mask that gets &-ed with hashes to determine which table the hash goes to.
  // This will take however many of the highest-order bits as we need for the current
  // number of tables.
  // It's a cached value of ONES_MASK<<shift_bits_.
  // (ONES_MASK has all bits set)
  uint32_t table_mask_;

  // Number of bits we shift ONES_MASK to get a mask for indexing into our current
  // array of tables.
  // this is 32 - log_2(num_tables_)
  int shift_bits_;

  // Array of buffers
  Buffer* buffers_;

  // Buffers for the buffers so we can write full cache lines.
  BufferBuffer* buffer_buffers_;
};

}

#endif

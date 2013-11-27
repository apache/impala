// This file now describes the interface for defining operations for our hash-store.
// Ultimately, function pointers will be passed in as parameters instead, but without
// codegen, we cannot assess efficiency with that interface because the calls can't be
// inlined.

#ifndef IMPALA_EXPERIMENTS_HASHING_TUPLE_TYPES_H
#define IMPALA_EXPERIMENTS_HASHING_TUPLE_TYPES_H

#include <inttypes.h>
#include <stdlib.h>
#include <smmintrin.h>

#include <glog/logging.h>

#include "common/compiler-util.h"

#define SIMPLE_AGG 1
#define COMPLEX_AGG 2
#define WORKLOAD COMPLEX_AGG

namespace impala {

// Interface
// =========
// This is the output tuple that the hash table stores. It contains the aggregation
// columns.
struct BuildTuple;
// This is the input tuple that has the table. It also has an entry for caching the hash.
struct ProbeTuple;
// Write to build the new BuildTuple that corresponds to probe.
void TupleConvert(const ProbeTuple* probe, BuildTuple* build);
// Returns true if probe matches build, else false
bool TupleEqual(const ProbeTuple* probe, const BuildTuple* build);
// Update build, reflecting the fact that probe exists.
// Returns true if probe should be inserted, else false.
// (Aggregation will probably always update and return false;
//  joins will probably unconditionally return true.)
bool TupleCombine(const ProbeTuple* probe, BuildTuple* build);
// Update tuple to have its hash computed.
void ComputeHash(ProbeTuple* tuple);
// Used for testing only. Generate n ProbeTuples.
// The number of build tuples that they reduce to should be base_id * multiplier.
// Multiplier is guaranteed to be a power of 2; if not, free to round it.
// The idea is to help with the math required to have the unlinked variation of several
// fields leading to exactly that number of build tuples.
ProbeTuple* GenTuples(uint64_t n, int base_id, int multiplier);

#if WORKLOAD == SIMPLE_AGG

// aggregating tuple stored in the hashtable
struct BuildTuple {
  uint32_t hash; // hash of the column we're grouping on (id)
  int32_t id; // id column, what we're aggregating on
  int64_t count; // current value of aggregate function (COUNT(*))
} __attribute__((__packed__));

// tuple from the db
struct ProbeTuple {
  int32_t id;
  uint32_t hash;
} __attribute__((__packed__));


inline void TupleConvert(const ProbeTuple* probe, BuildTuple* build) {
  build->hash = probe->hash;
  build->id = probe->id;
  build->count = 1;
}

inline bool TupleEqual(const ProbeTuple* probe, const BuildTuple* build) {
  return probe->id == build->id;
}

inline bool TupleCombine(const ProbeTuple* probe, BuildTuple* build) {
  if (LIKELY(TupleEqual(probe, build))) {
    ++build->count;
    return false;
  } else {
    // Probe isn't the same as build, so insert a new tuple instead of combining.
    return true;
  }
}

inline void ComputeHash(ProbeTuple* tuple) {
  tuple->hash = _mm_crc32_u32(0, tuple->id);
}


// Generate n random tuples, with ids in the range [0, base_id * multiplier).
inline ProbeTuple* GenTuples(uint64_t n, int base_id, int multiplier) {
  ProbeTuple* tuples
    = static_cast<ProbeTuple*>(malloc(n * sizeof(ProbeTuple)));

  CHECK(tuples != NULL);

  for (uint64_t i = 0; i < n; ++i) {
    tuples[i].id = rand() % (base_id * multiplier);
  }
  return tuples;
}

#elif WORKLOAD == COMPLEX_AGG

// aggregating tuple stored in the hashtable
struct BuildTuple {
  uint32_t hash;
  int32_t cpd_dt;
  int32_t mrch_corp_nm;
  int64_t mrch_catg_cd;
  int64_t tran_id;
  double us_tran_amt;
  int64_t count; // current value of aggregate function COUNT(*)
  int64_t sum; // current value of aggregate function SUM
} __attribute__((__packed__));

// tuple from the db
struct ProbeTuple {
  uint32_t hash;
  int32_t cpd_dt;
  int32_t mrch_corp_nm;
  int64_t mrch_catg_cd;
  int64_t tran_id;
  double us_tran_amt;
} __attribute__((__packed__));


inline bool TupleEqual(const ProbeTuple* probe, const BuildTuple* build) {
  return probe->cpd_dt == build->cpd_dt && probe->mrch_corp_nm == build->mrch_corp_nm
    && probe->mrch_catg_cd == build->mrch_catg_cd && probe->tran_id == build->tran_id;
}

inline void TupleConvert(const ProbeTuple* probe, BuildTuple* build) {
  build->hash = probe->hash;
  build->count = 1;
  build->sum = probe->us_tran_amt;
  build->cpd_dt = probe->cpd_dt;
  build->mrch_corp_nm = probe->mrch_corp_nm;
  build->mrch_catg_cd = probe->mrch_catg_cd;
  build->tran_id = probe->tran_id;
  build->us_tran_amt = probe->us_tran_amt;
}

inline bool TupleCombine(const ProbeTuple* probe, BuildTuple* build) {
  if (LIKELY(TupleEqual(probe, build))) {
    ++build->count;
    build->sum += probe->us_tran_amt;
    return false;
  } else {
    // Probe isn't the same as build, so insert a new tuple instead of combining.
    return true;
  }
}

// From util/hash-util.h
// Compute the Crc32 hash for data using SSE4 instructions.  The input hash parameter is 
// the current hash/seed value.
// This should only be called if SSE is supported.
// This is ~4x faster than Fnv/Boost Hash.
static inline uint32_t CrcHash(const void* data, int32_t bytes, uint32_t hash) {
  uint32_t words = bytes / sizeof(uint32_t);
  bytes = bytes % sizeof(uint32_t);

  const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
  while (words--) {
    hash = _mm_crc32_u32(hash, *p);
    ++p;
  }

  const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
  while (bytes--) {
    hash = _mm_crc32_u8(hash, *s);
    ++s;
  }

  return hash;
}


inline void ComputeHash(ProbeTuple* tuple) {
  tuple->hash = CrcHash((void*)&tuple->cpd_dt,
                        sizeof(*tuple) - sizeof(tuple->hash) - sizeof(tuple->us_tran_amt),
                        12);
}

// returns log base 2 of n, rounded up.
static int log_base_2(int n) {
  DCHECK(n > 0);
  int i = 0;
  while (n > 1) {
    n /= 2;
    ++i;
  }
  return i;
}

// Generate n random tuples, with ids in the range [0, base_id * multiplier).
// Multiplier is power of 2.
inline ProbeTuple* GenTuples(uint64_t n, int base_id, int multiplier) {
  ProbeTuple* tuples
    = static_cast<ProbeTuple*>(malloc(n * sizeof(ProbeTuple)));

  CHECK(tuples != NULL);

  int multiplier_bits = log_base_2(multiplier);

  for (uint64_t i = 0; i < n; ++i) {
    ProbeTuple* tuple = &tuples[i];
    // grouped_by columns. Need to respect max_id
    tuple->cpd_dt = rand() % base_id;
    tuple->mrch_corp_nm = rand() % (1<<((multiplier_bits + 2) / 3));
    tuple->mrch_catg_cd = rand() % (1<<((multiplier_bits + 1) / 3));
    tuple->tran_id = rand() % (1<<(multiplier_bits / 3));

    tuple->us_tran_amt = rand() % 1000;
  }

  return tuples;
}


#endif // WORKLOAD



}

#endif

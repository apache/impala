// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HASH_TABLE_H
#define IMPALA_EXEC_HASH_TABLE_H

#include <vector>
#include <functional>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <boost/functional/hash.hpp>

namespace impala {

class Expr;
class RowDescriptor;
class Tuple;
class TupleDescriptor;
class TupleRow;

// A hash table (a multi-set) that indexes Tuple* by a set of expressions over the
// element tuples (ie, the hash value computation and equality test is done on the
// values returned by those exprs).
// There are two separate sets of expressions:
// - build exprs: these are evaluated when tuples are inserted into the table
// - probe exprs: these are evaluated when trying to look up particular values
class HashTable {
 public:
  // Construct a new hash table. If stores_nulls is true, the hash table
  // stores tuples for which build_exprs return NULLs and will consider
  // NULL == NULL when doing a scan.
  // If stores_nulls is false, the hash table will (silently) reject
  // tuples for which build_exprs return NULLs.
  HashTable(const std::vector<Expr*>& build_exprs,
            const std::vector<Expr*>& probe_exprs,
            const RowDescriptor& build_row_desc,
            int build_tuple_idx,  // pos. of build tuple w/in build row
            bool stores_nulls);

  // Inserts t by evaluating build exprs. If !stores_nulls and one of the
  // build exprs return a NULL, returns w/o inserting t.
  void Insert(Tuple* t);

  void DebugString(int indentation_level, std::stringstream* out) const;
  
 private:
  class HashFn : public std::unary_function<Tuple*, std::size_t> {
   public:
    HashFn(HashTable* ht): hash_tbl_(ht) {}

    // Compute a combined hash value for the values returned by the build
    // or probe exprs, depending on whether hash_tbl_->current_build_row_
    // or current_probe_row_ is set. 
    // If t is non-NULL, hash values are always computed from build exprs.
    std::size_t operator()(Tuple* const& t) const;

   private:
    HashTable* hash_tbl_;
  };

  class EqualsFn : public std::binary_function<Tuple*, Tuple*, bool> {
   public:
    EqualsFn(HashTable* ht): hash_tbl_(ht) {}

    // Return true if values of build or probe exprs in the context of a
    // are the same as the values of build exprs in the context of b, otherwise false.
    // If a is NULL, computes the values used for comparison from
    // HashTable::current_build_row_ or current_probe_row_, depending on which is
    // set.
    bool operator()(Tuple* const& a, Tuple* const& b) const;

   private:
    HashTable* hash_tbl_;
  };

  friend class HashFn;
  friend class EqualsFn;

  typedef boost::unordered_multiset<Tuple*, HashFn, EqualsFn> HashSet;

  HashFn hash_fn_;
  EqualsFn equals_fn_;
  boost::scoped_ptr<HashSet> hash_tbl_;
  const std::vector<Expr*> build_exprs_;
  const std::vector<Expr*> probe_exprs_;
  TupleDescriptor* build_tuple_desc_;

  // needed for Hash-/EqualsFn
  TupleRow* current_build_row_;
  TupleRow* current_probe_row_;

  // row in which to embed stored Tuple* for the purpose of evaluating exprs
  boost::scoped_array<Tuple*> build_row_data_;
  TupleRow* build_row_;

  // pos. w/in a build row for the purpose of evaluating build_exprs_
  int build_tuple_idx_; 

  bool stores_nulls_;

 public:
  class Iterator {
   public:
    // Returns next matching element or NULL;
    Tuple* GetNext() {
      if (i_ == end_) return NULL;
      return *i_++;
    }

   private:
    friend class HashTable;
    HashSet::iterator i_;
    HashSet::iterator end_;

    void Reset(const std::pair<HashSet::iterator, HashSet::iterator>& range) {
      i_ = range.first;
      end_ = range.second;
    }
  };

  // Starts as a scan of tuples based on values of probe_exprs in the context
  // of probe_row. Scans entire table if probe_row is NULL.
  // Returns the scan through 'it'.
  void Scan(TupleRow* probe_row, Iterator* it);

  std::string DebugString();

};

}

#endif

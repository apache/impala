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

// A hash table (a multi-set) that indexes TupleRow* by a set of expressions over the
// element tuples (ie, the hash value computation and equality test is done on the
// values returned by those exprs).
// There are two separate sets of expressions:
// - build exprs: these are evaluated when rows are inserted into the table
// - probe exprs: these are evaluated when trying to look up particular values
class HashTable {
 public:
  // Construct a new hash table. If stores_nulls is true, the hash table
  // stores rows for which build_exprs return NULLs and will consider
  // NULL == NULL when doing a scan.
  // If stores_nulls is false, the hash table will (silently) reject
  // rows for which build_exprs return NULLs.
  HashTable(const std::vector<Expr*>& build_exprs,
            const std::vector<Expr*>& probe_exprs,
            const RowDescriptor& build_row_desc,
            bool stores_nulls);

  // Inserts r by evaluating build exprs. If !stores_nulls and one of the
  // build exprs return a NULL, returns w/o inserting t.
  void Insert(TupleRow* r);

  void DebugString(int indentation_level, std::stringstream* out) const;

  int size() const { return hash_tbl_->size(); }

 private:
  class HashFn : public std::unary_function<TupleRow*, std::size_t> {
   public:
    HashFn(HashTable* ht): hash_tbl_(ht) {}

    // Compute a combined hash value for the values returned by the build
    // or probe exprs.
    // If r is non-NULL, hash values are always computed from build exprs.
    // If r is NULL, hash values are always computed from probe exprs over
    // current_probe_row_.
    std::size_t operator()(TupleRow* const& r) const;

   private:
    HashTable* hash_tbl_;
  };

  class EqualsFn : public std::binary_function<TupleRow*, TupleRow*, bool> {
   public:
    EqualsFn(HashTable* ht): hash_tbl_(ht) {}

    // Return true if values of build or probe exprs in the context of a
    // are the same as the values of build exprs in the context of b, otherwise false.
    // If a is NULL, computes the 'a' values used for comparison by evaluating
    // probe_exprs_ over current_probe_row_.
    // If a is not NULL, computes the 'a' values used for comparison by evaluating
    // probe exprs over 'a'.
    bool operator()(TupleRow* const& a, TupleRow* const& b) const;

   private:
    HashTable* hash_tbl_;
  };

  friend class HashFn;
  friend class EqualsFn;

  typedef boost::unordered_multiset<TupleRow*, HashFn, EqualsFn> HashSet;

  HashFn hash_fn_;
  EqualsFn equals_fn_;
  boost::scoped_ptr<HashSet> hash_tbl_;
  const std::vector<Expr*> build_exprs_;
  const std::vector<Expr*> probe_exprs_;
  const RowDescriptor& build_row_desc_;

  // The probe_row given in Scan
  TupleRow* current_probe_row_;

  bool stores_nulls_;

 public:
  class Iterator {
   public:
    // Returns next matching element or NULL;
    TupleRow* GetNext() {
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

  // Starts as a scan of rows based on values of probe_exprs in the context
  // of probe_row. Scans entire table if probe_row is NULL.
  // Returns the scan through 'it'.
  void Scan(TupleRow* probe_row, Iterator* it);

  std::string DebugString();

};

}

#endif

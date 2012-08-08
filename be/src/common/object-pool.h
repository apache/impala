// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_OBJECT_POOL_H
#define IMPALA_COMMON_OBJECT_POOL_H

#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace impala {

// An ObjectPool maintains a list of C++ objects which are deallocated
// by destroying the pool.
// Thread-safe.
class ObjectPool {
 public:
  ObjectPool(): objects_() {}

  ~ObjectPool() {
    for (ElementVector::iterator i = objects_.begin();
         i != objects_.end(); ++i) {
      delete *i;
    }
  }

  template <class T>
  T* Add(T* t) {
    boost::lock_guard<boost::mutex> l(lock_);
    objects_.push_back(new SpecificElement<T>(t));
    return t;
  }

 private:
  struct GenericElement {
    virtual ~GenericElement() {}
  };

  template <class T>
  struct SpecificElement : GenericElement {
    SpecificElement(T* t): t(t) {}
    ~SpecificElement() {
      delete t;
    }

    T* t;
  };

  typedef std::vector<GenericElement*> ElementVector;
  ElementVector objects_;
  boost::mutex lock_;
};

}

#endif

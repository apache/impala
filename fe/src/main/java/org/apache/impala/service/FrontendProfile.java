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
package org.apache.impala.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Wrapper class for creating a runtime profile within the frontend.
 *
 * In order to avoid plumbing an object through all code that might want to emit counters
 * into the profile, this class provides some support for storing a current profile in
 * a thread-local variable.
 *
 * This class is thread-safe.
 */
@ThreadSafe
public class FrontendProfile {
  private static final String ROOT_COUNTER_NAME = "";

  private static ThreadLocal<FrontendProfile> THREAD_LOCAL =
      new ThreadLocal<>();

  @GuardedBy("this")
  private TRuntimeProfileNode profile_;

  /**
   * Name-based access to the counters in the profile_.counters List<TCounter>.
   */
  @GuardedBy("this")
  private final Map<String, TCounter> countersByName_ = new HashMap<>();

  FrontendProfile() {
    profile_ = new TRuntimeProfileNode("Frontend",
        /*num_children=*/ 0,
        /*counters=*/new ArrayList<>(),
        /*metadata=*/-1L, // TODO(todd) what is this used for? why is it required?
        /*indent=*/false,
        /*info_strings=*/new HashMap<>(),
        /*info_strings_display_order*/new ArrayList<>(),
        /*child_counters_map=*/ImmutableMap.of(ROOT_COUNTER_NAME, new HashSet<>()));
  }

  /**
   * Create a new profile, setting it as the current thread-local profile for the
   * length of the current scope. This is meant to be used in a try-with-resources
   * statement. Supports at most one scope per thread. No nested scopes are currently
   * allowed.
   */
  public static Scope createNewWithScope() {
    return new Scope(new FrontendProfile());
  }

  /**
   * Get the profile attached to the current thread, throw IllegalStateException if there
   * is none.
   */
  @Nonnull
  public static FrontendProfile getCurrent() {
    FrontendProfile prof = THREAD_LOCAL.get();
    Preconditions.checkState(prof != null, "no profile in scope");
    return prof;
  }

  /**
   * Get the profile attached to the current thread, or null if there is no current
   * profile.
   */
  @Nullable
  public static FrontendProfile getCurrentOrNull() {
    return THREAD_LOCAL.get();
  }

  /**
   * Return the profile in Thrift format. This may be called only once, and after it is
   * called, no further methods may be used on this PlannerProfile object. Any attempts
   * to do so will result in IllegalStateExceptions.
   */
  public synchronized TRuntimeProfileNode emitAsThrift() {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    TRuntimeProfileNode ret = profile_;
    profile_ = null;
    return ret;
  }

  /**
   * Add an informational key/value string pair to the profile. These are written out
   * as is to the user. Subsequent calls with the same key will overwrite previous ones.
   */
  public synchronized void addInfoString(String key, String val) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(val);
    if (profile_.getInfo_strings().put(key, val) == null) {
      // If it's a new info string instead of replacing an existing one,
      // we need to also include it in the 'ordering' list.
      profile_.getInfo_strings_display_order().add(key);
    }
  }

  /**
   * Add 'delta' to the counter with the given name and unit. Counters are created
   * on-demand.
   */
  public synchronized void addToCounter(String name, TUnit unit, long delta) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    TCounter counter = countersByName_.get(Preconditions.checkNotNull(name));
    if (counter == null) {
      // Need to create the counter.
      counter = new TCounter(name, unit, 0);
      countersByName_.put(name, counter);
      profile_.counters.add(counter);
      // Currently we don't support hierarchical counters in the frontend.
      profile_.child_counters_map.get(ROOT_COUNTER_NAME).add(name);
    }
    counter.value += delta;
  }


  public static class Scope implements AutoCloseable {
    private final FrontendProfile oldThreadLocalValue_;

    private Scope(FrontendProfile profile) {
      oldThreadLocalValue_ = THREAD_LOCAL.get();
      // TODO: remove when allowing nested scopes.
      Preconditions.checkState(oldThreadLocalValue_ == null);
      THREAD_LOCAL.set(profile);
    }

    @Override
    public void close() {
      THREAD_LOCAL.set(oldThreadLocalValue_);
    }
  }
}

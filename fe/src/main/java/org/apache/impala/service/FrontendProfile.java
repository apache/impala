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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Lists;
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

  /**
   * Additional profile node to be displayed under {@link #profile_}.
   */
  @GuardedBy("this")
  private Map<String, TRuntimeProfileNode> childrenProfiles_ = new TreeMap<>();

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
   * Return the Frontend profile in Thrift format.
   * <p>
   * This may be called only once, and after it is called, no further methods may be used
   * on this PlannerProfile object except {@link #emitChildrenAsThrift()}. Any attempts to
   * do so will result in IllegalStateExceptions.
   */
  public synchronized TRuntimeProfileNode emitAsThrift() {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    TRuntimeProfileNode ret = profile_;
    profile_ = null;
    return ret;
  }

  /**
   * Return the Frontend's children profiles in Thrift format.
   * <p>
   * {@link #emitAsThrift()} must be called ahead of this method.
   * This may be called only once, and after it is called, no further methods may be used
   * on this PlannerProfile object. Any attempts to do so will result in
   * IllegalStateExceptions.
   */
  public synchronized List<TRuntimeProfileNode> emitChildrenAsThrift() {
    Preconditions.checkState(profile_ == null, "emitAsThrift() must be called first");
    Preconditions.checkState(childrenProfiles_ != null, "already emitted profile");
    List<TRuntimeProfileNode> ret = Lists.newArrayList(childrenProfiles_.values());
    childrenProfiles_ = null;
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
   * Appends an informational key/value string pair to the profile. These are written out
   * as is to the user. Values are appended to a comma separated list of values.
   */
  public synchronized void appendInfoString(String key, String val) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(val);
    Map<String, String> info_strings = profile_.getInfo_strings();
    if (info_strings.containsKey(key)) {
      info_strings.put(key, info_strings.get(key) + ", " + val);
    } else {
      info_strings.put(key, val);
      profile_.getInfo_strings_display_order().add(key);
    }
  }

  /**
   * Returns the info string associated with the given key. Returns an empty String if
   * the key does not exist.
   */
  public synchronized String getInfoString(String key) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    Preconditions.checkNotNull(key);
    return profile_.getInfo_strings().getOrDefault(key, "");
  }

  /**
   * Add 'child' profile under 'Frontend' profile node in query profile.
   */
  public synchronized void addChildrenProfile(TRuntimeProfileNode child) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    Preconditions.checkState(childrenProfiles_ != null, "already emitted profile");
    Preconditions.checkNotNull(child.getName());
    Preconditions.checkArgument(!childrenProfiles_.containsKey(child.getName()));
    childrenProfiles_.put(child.getName(), child);
  }

  private TCounter getOrCreateCounter(String name, TUnit unit) {
    TCounter counter = countersByName_.get(Preconditions.checkNotNull(name));
    if (counter == null) {
      // Need to create the counter.
      counter = new TCounter(name, unit, 0);
      countersByName_.put(name, counter);
      profile_.counters.add(counter);
      // Currently we don't support hierarchical counters in the frontend.
      profile_.child_counters_map.get(ROOT_COUNTER_NAME).add(name);
    }
    return counter;
  }

  /**
   * Add 'delta' to the counter with the given name and unit. Counters are created
   * on-demand.
   */
  public synchronized void addToCounter(String name, TUnit unit, long delta) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    TCounter counter = getOrCreateCounter(name, unit);
    counter.value += delta;
  }

  /**
   * Set 'value' to the counter with the given name. Counters are created
   * on-demand.
   */
  public synchronized void setToCounter(String name, TUnit unit, long value) {
    Preconditions.checkState(profile_ != null, "already emitted profile");
    TCounter counter = getOrCreateCounter(name, unit);
    counter.value = value;
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

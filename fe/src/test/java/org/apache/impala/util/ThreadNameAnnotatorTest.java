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

package org.apache.impala.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ThreadNameAnnotatorTest {

  /**
   * Simple synchronizer that forces two threads to work in
   * lock-step by stepping from one state to the next.
   * Each thread waits for a state, does something, sets the
   * new state, and waits for the other thread to advance the
   * state again.
   */
  private static class Stepper {
    private String state_ = "<start>";

    public synchronized void setState(String state) {
      state_ = state;
      notifyAll();
    }

    public synchronized void waitForState(String state) {
      for (;;) {
        if (state_.equals(state)) return;
        try {
          wait(15_000);
        } catch (InterruptedException e) {
          fail("Test timed out, likely test flow error");
        }
      }
    }

    public void advance(String newState, String waitFor) throws InterruptedException {
      setState(newState);
      waitForState(waitFor);
    }
  }

  /**
   * Thread which uses the thread annotator to rename itself and a stepper
   * to synchronize with the test thread.
   */
  public static class AnnotatedThread extends Thread {
    private final Stepper stepper_;

    public AnnotatedThread(Stepper stepper) {
      stepper_ = stepper;
    }

    public AnnotatedThread(String name, Stepper stepper) {
      super(name);
      stepper_ = stepper;
    }

    @Override
    public void run() {
      try {
        stepper_.advance("before", "annotate");
        try (ThreadNameAnnotator tna = new ThreadNameAnnotator("annotated")) {
          stepper_.advance("annotated","continue");
        }
        stepper_.advance("after", "done");
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  /**
   * Verify that the thread annotator works for the basic case.
   */
  @Test
  public void testAnnotator() throws InterruptedException {
    Stepper stepper = new Stepper();
    // Thread with name "orig"
    Thread thread = new AnnotatedThread("orig", stepper);
    assertEquals("orig", thread.getName());
    thread.start();
    stepper.waitForState("before");
    assertEquals("orig", thread.getName());
    stepper.advance("annotate", "annotated");
    // Annotator should have added our "annotated" annotation
    assertEquals("orig [annotated]", thread.getName());
    stepper.advance("continue", "after");
    // Original thread name should be restored
    assertEquals("orig", thread.getName());
    stepper.setState("done");
    thread.join();
    assertEquals("orig", thread.getName());
  }

  /**
   * Test that the annotator will not restore the original thread name
   * if the thread name is changed out from under the annotator.
   */
  @Test
  public void testExternalRename() throws InterruptedException {
    Stepper stepper = new Stepper();
    Thread thread = new AnnotatedThread("orig", stepper);
    assertEquals("orig", thread.getName());
    thread.start();
    stepper.waitForState("before");
    assertEquals("orig", thread.getName());
    stepper.advance("annotate", "annotated");
    // Thread is annotated as before
    assertEquals("orig [annotated]", thread.getName());
    // Thread name is changed out from under the annotator
    thread.setName("gotcha");
    stepper.advance("continue", "after");
    // Annotator noticed the change, did not restore original name
    assertEquals("gotcha", thread.getName());
    stepper.setState("done");
    thread.join();
    assertEquals("gotcha", thread.getName());
  }

  /**
   * Test the degenerate case that the original thread name is the one
   * assigned by Java.
   */
  @Test
  public void testDefaultName() throws InterruptedException {
    Stepper stepper = new Stepper();
    Thread thread = new AnnotatedThread(stepper);
    String orig = thread.getName();
    thread.start();
    stepper.waitForState("before");
    assertEquals(orig, thread.getName());
    stepper.advance("annotate", "annotated");
    assertEquals(orig + " [annotated]", thread.getName());
    stepper.advance("continue", "after");
    assertEquals(orig, thread.getName());
    stepper.setState("done");
    thread.join();
    assertEquals(orig, thread.getName());
  }
}

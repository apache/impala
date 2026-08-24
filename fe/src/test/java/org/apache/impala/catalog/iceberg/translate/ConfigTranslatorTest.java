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

package org.apache.impala.catalog.iceberg.translate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for the shared {@link ConfigTranslator} engine and its rules. */
public class ConfigTranslatorTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /** Builds a single-rule translator that drops unrecognized keys. */
  private static ConfigTranslator translator(TranslationRule rule) {
    return new ConfigTranslator(
        List.of(rule), /*passThroughRemaining*/ false);
  }

  /** Runs 'rule' against 'source' and returns the translated output. */
  private static Map<String, String> translate(
      TranslationRule rule, Map<String, String> source) {
    return translator(rule).translate(source);
  }

  @Test
  public void testRenameByCanonicalKey() {
    TranslationRule rule = new TranslationRule.RenameRule("uri", List.of("alt.uri"));
    assertEquals("x", translate(rule, ImmutableMap.of("uri", "x")).get("uri"));
  }

  @Test
  public void testRenameByAlias() {
    TranslationRule rule = new TranslationRule.RenameRule("uri", List.of("alt.uri"));
    Map<String, String> out = translate(rule, ImmutableMap.of("alt.uri", "x"));
    assertEquals("x", out.get("uri"));
    assertFalse(out.containsKey("alt.uri"));
  }

  @Test
  public void testRenameAmbiguityThrows() {
    TranslationRule rule = new TranslationRule.RenameRule("uri", List.of("alt.uri"));
    expectedException.expect(IllegalStateException.class);
    translate(rule, ImmutableMap.of("uri", "x", "alt.uri", "y"));
  }

  @Test
  public void testRequiredPresent() {
    TranslationRule rule = new TranslationRule.RequiredRule("uri");
    assertEquals("x", translate(rule, ImmutableMap.of("uri", "x")).get("uri"));
  }

  @Test
  public void testRequiredMissingThrows() {
    TranslationRule rule = new TranslationRule.RequiredRule("uri");
    expectedException.expect(IllegalStateException.class);
    translate(rule, ImmutableMap.of());
  }

  @Test
  public void testVerifyCaseInsensitiveSucceeds() {
    TranslationRule rule = new TranslationRule.VerifyRule("session", "NONE");
    assertEquals(
        "none", translate(rule, ImmutableMap.of("session", "none")).get("session"));
  }

  @Test
  public void testVerifyMismatchThrows() {
    TranslationRule rule = new TranslationRule.VerifyRule("session", "NONE");
    expectedException.expect(IllegalStateException.class);
    translate(rule, ImmutableMap.of("session", "user"));
  }

  @Test
  public void testIgnoreDropsKey() {
    TranslationRule rule = new TranslationRule.IgnoreRule("connector.name");
    assertTrue(translate(rule, ImmutableMap.of("connector.name", "iceberg")).isEmpty());
  }

  @Test
  public void testMapRename() {
    TranslationRule rule = new TranslationRule.MapRenameRule(ImmutableMap.of(
        "s3.access-key-id", "fs.s3a.access.key",
        "s3.secret-access-key", "fs.s3a.secret.key"));
    Map<String, String> out = translate(rule, ImmutableMap.of(
        "s3.access-key-id", "AKID", "s3.secret-access-key", "SECRET"));
    assertEquals("AKID", out.get("fs.s3a.access.key"));
    assertEquals("SECRET", out.get("fs.s3a.secret.key"));
    assertEquals(2, out.size());
  }

  @Test
  public void testProgrammaticSeesConsumedKeys() {
    // The MapRenameRule drains "s3.session-token" before the ProgrammaticRule runs, but
    // the ProgrammaticRule must still observe it via the original source snapshot.
    ConfigTranslator t = new ConfigTranslator(
        ImmutableList.of(
            new TranslationRule.MapRenameRule(
                ImmutableMap.of("s3.session-token", "fs.s3a.session.token")),
            new TranslationRule.ProgrammaticRule((source, output) -> {
              if (source.containsKey("s3.session-token")) {
                output.put("fs.s3a.aws.credentials.provider", "Temporary");
              }
            })),
        /*passThroughRemaining*/ false);
    Map<String, String> out = t.translate(ImmutableMap.of("s3.session-token", "TOKEN"));
    assertEquals("TOKEN", out.get("fs.s3a.session.token"));
    assertEquals("Temporary", out.get("fs.s3a.aws.credentials.provider"));
  }

  @Test
  public void testProgrammaticSkippedWhenAbsent() {
    TranslationRule rule = new TranslationRule.ProgrammaticRule((source, output) -> {
      if (source.containsKey("s3.session-token")) {
        output.put("fs.s3a.aws.credentials.provider", "Temporary");
      }
    });
    assertTrue(translate(rule, ImmutableMap.of("s3.access-key-id", "AKID")).isEmpty());
  }

  @Test
  public void testPassThroughRemainingTrueKeepsUnknownKeys() {
    ConfigTranslator t = new ConfigTranslator(
        ImmutableList.of(new TranslationRule.RenameRule("uri")),
        /*passThroughRemaining*/ true);
    Map<String, String> out = t.translate(ImmutableMap.of("uri", "x", "unknown", "y"));
    assertEquals("x", out.get("uri"));
    assertEquals("y", out.get("unknown"));
  }

  @Test
  public void testPassThroughRemainingFalseDropsUnknownKeys() {
    Map<String, String> out = translate(
        new TranslationRule.RenameRule("uri"),
        ImmutableMap.of("uri", "x", "unknown", "y"));
    assertEquals("x", out.get("uri"));
    assertFalse(out.containsKey("unknown"));
  }

  @Test
  public void testSourceNotMutated() {
    Map<String, String> source = ImmutableMap.of("uri", "x");
    translate(new TranslationRule.RenameRule("uri"), source);
    assertEquals("x", source.get("uri"));
    assertEquals(1, source.size());
  }
}

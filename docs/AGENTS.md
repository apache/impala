<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Documentation Standards

All documentation contributors MUST follow these standards to ensure consistency,
clarity, and accessibility across Impala documentation.

## Content

- MUST document new features, behavioral changes.
- SHOULD keep it short and clear.
- MUST explain the requirements for the desired outcome. SHOULD elaborate on error paths
  only if the behavior is unexpected e.g. fallback, silent ignore.
- MUST avoid redundancy. Give examples only for the syntax or if the description needs
  more clarity.
- MAY recommend configurations for performance.

## Voice and Tone Standards

- MUST use second person ("you") and present tense.
- MUST use active voice.
- Example: "You can configure Impala to..." (not "Impala can be configured to...")

### Phrasing Patterns by Context

| Context | Pattern | Example |
|---------|---------|---------|
| Possibility | You can + verb | You can configure Impala to store binary data. |
| Requirement | You must + verb | You must set the option to true. |
| Recommendation | Impala recommends + verb-ing | Impala recommends considering performance guidelines. |
| Instruction | Imperative (verb first) | Set the option. Run the command. |

### Avoid Anthropomorphism

- MUST NOT assign human characteristics to software.
  -  "The script will tell you if Impala is up to date"
  -  "Run the script to check if you have the latest Impala version"

## Banned Words and Preferred Alternatives

### Inclusive Terminology

| Banned Term | Preferred Alternative |
|---|---|
| man hours | person-hours, staff hours |
| man-in-the-middle | Use full descriptive term |
| he/she/he or she | you, they, them, their |
| The user, The end-user | You |
| The administrator | You |

### Master/Slave Terminology

| Banned Term | Preferred Alternative |
|---|---|
| master (node/server) | primary, source, leader, control plane |
| slave (node/server) | secondary, replica, follower, worker |
| master branch | main branch, source branch |
| master key/copy | primary, source, parent, original |

**Exception**: It is acceptable to use "master" when referring to the Impala repository's
primary upstream branch, as this is the official branch name on Apache's infrastructure.

### Blacklist/Whitelist Terminology

| Banned Term | Preferred Alternative | Verb Form |
|---|---|---|
| blacklist | denylist, blocked list, exclude list | reject, exclude, disallow |
| whitelist | allowlist, trusted list, include list | allow, authenticate, include |

### System Behavior Terminology

| Banned Term | Preferred Alternative | Example |
|---|---|---|
| execute/execution | run, use | "Run the command" |
| hang | not responding, freezes | "The connection stops responding" |
| dummy | placeholder | "Create placeholder files" |
| may (for possibility) | might | "The query might timeout" |
| should | must (requirements), might (possibility) | "You must configure this" |

### Grammar and Language Standards

- MUST use US spelling: color (not colour), optimize (not optimise).
- MUST NOT use "please" and "thank you" in procedural documentation.
- MUST use gender-neutral language throughout.
- MUST scope statements to actual components (not generalize behavior).
- SHOULD use articles correctly: omit before plural/product names; use "the" before specific nouns.
- SHOULD avoid excessive prepositions. Rewrite for clarity when needed.
- MUST maintain parallel structure in lists and series.

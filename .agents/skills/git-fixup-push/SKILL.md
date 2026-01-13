---
name: git-fixup-push
description: Combines all git commits for a change into a single commit and pushes it to gerrit. Use when you push to gerrit.
license: Apache-2.0
metadata:
  version: "1.0"
---

## Fixup Git Commits and Push to Gerrit

Use git-fixup-push when you have multiple commits for a change and want to combine them
into a single commit before pushing to gerrit. Changes to Impala are managed in gerrit,
and each change is represented as a single git commit. Use git-fixup-push to meld multiple
commits into a single commit that can be pushed to gerrit.

## Deterministic Execution Contract

- Run all commands from the repository root in a single shell session.
- Use non-interactive commands only.
- Treat any non-zero command exit code as a failure for the current step unless the step
  explicitly defines recovery behavior.
- Do not use force operations (`--force`, `--force-with-lease`, hard reset).
- Do not run `git remote -v`.
- Ignore untracked files for staged/unstaged detection and for stashing decisions.

## Canonical Checks

- Staged changes exist: `git diff --cached --quiet` (exit code `1` means staged changes exist).
- Unstaged tracked changes exist: `git diff --quiet` (exit code `1` means unstaged tracked
  changes exist).
- Most recent commit message: `git log -1 --pretty=%B`.
- Valid gerrit username regex: `^[A-Za-z0-9._-]+$`.
- Remote URL checks:
  - Fetch URL: `git remote get-url asf-gerrit`
  - Push URL: `git remote get-url --push asf-gerrit`

## Available Scripts

- `.agents/skills/git-fixup-push/scripts/fixup.sh` - Script to combine multiple
  commits into a single commit.
  Run with the `--help` argument to see usage instructions. Run with the `--dry-run`
  argument to see the exact command that would be executed without actually running it.
  The script selects commits from the most recent `IMPALA-<id>` commit through `HEAD`.
  If that matching commit is already `HEAD`, the script exits without squashing.

## Workflow

- [ ] Step 1: Validate `asf-gerrit` remote and URLs.
  - Success condition: both effective URLs exactly match
    `ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`.
  - Procedure:
    - Try `git remote get-url asf-gerrit` and `git remote get-url --push asf-gerrit`.
    - If remote does not exist, ask once for `<username>`.
    - Validate `<username>` against `^[A-Za-z0-9._-]+$`.
    - If `<username>` is invalid, abort.
    - If remote does not exist and `<username>` is valid, run:
      - `git remote add asf-gerrit ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`
    - If remote exists but URL mismatch is detected, ask once for `<username>`.
    - Validate `<username>` against `^[A-Za-z0-9._-]+$`.
    - If `<username>` is invalid, abort.
    - If remote exists with URL mismatch and `<username>` is valid, run:
      - `git remote set-url asf-gerrit ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`
      - `git remote set-url --push asf-gerrit ssh://<username>@gerrit.cloudera.org:29418/Impala-ASF`
    - Re-run both URL checks. If either still mismatches, abort.
- [ ] Step 2: Commit already-staged changes (pre-existing index state).
  - Run staged check.
  - If staged changes exist, run:
    - `git commit -m "WIP: commit staged files"`
  - If no staged changes, continue.
- [ ] Step 3: Resolve unstaged tracked changes.
  - Run unstaged tracked check.
  - If no unstaged tracked changes, continue.
  - If unstaged tracked changes exist, ask user to choose exactly one action:
    - `stage`: run exactly `git add -u`
    - `stash`: run `git stash push --keep-index -m "stashed by git-fixup-push skill"`.
      Record that a stash was created; Step 9 will pop it after a successful push.
  - If input is not exactly `stage` or `stash`, abort.
- [ ] Step 4: Commit staged changes created by Step 3.
  - Run staged check again.
  - If staged changes exist, run:
    - `git commit -m "WIP: commit staged files"`
  - If no staged changes, continue.
- [ ] Step 5: Squash commits for the current change.
  - Run: `.agents/skills/git-fixup-push/scripts/fixup.sh`
  - If this command exits non-zero, abort.
- [ ] Step 6: Run critique with bounded retry policy.
  - Run from `${IMPALA_HOME}` exactly:
    - `./bin/jenkins/critique-gerrit-review.py --dryrun`
  - If critique passes, continue.
  - If critique fails on first attempt:
    - For each file and line number listed in the critique output, apply exactly the
      code-style fix described (for example: trim trailing whitespace, fix indentation,
      shorten lines to ≤90 characters). Do not make unrelated changes.
    - Commit fixes with:
      - `git commit -m "fixed critique issues"`
    - Restart workflow from Step 1 and allow exactly one additional critique attempt.
  - If critique fails on the second attempt, abort.
- [ ] Step 7: Ensure `Assisted-by:` trailer is present on `HEAD`.
  - Check trailers with:
    - `git log -1 --pretty=%B | git interpret-trailers --only-trailers --unfold`
  - If an `Assisted-by:` trailer is already present, do nothing and continue.
  - If missing, ask user whether to add it.
  - If user approves, amend commit message using `git interpret-trailers` and add exactly:
    - `Assisted-by: <model> (<agent-name>)` where `<model>` is the name of the language
      model and `<agent-name>` is the name of the agent executing this skill.
  - Deterministic amend sequence:
    - `tmp_msg="$(mktemp)"`
    - `git log -1 --pretty=%B | git interpret-trailers --if-exists=doNothing --trailer "Assisted-by: <model> (<agent-name>)" > "$tmp_msg"`
    - `git commit --amend -F "$tmp_msg"`
    - `rm -f "$tmp_msg"`
  - Preserve Gerrit footer requirements:
    - Keep `Change-Id:` in the final trailer paragraph.
    - Keep trailer lines contiguous (no blank lines between trailer entries).
  - If user declines, continue.
  - If user input is invalid, abort.
- [ ] Step 8: Push to Gerrit drafts.
  - Run exactly:
    - `git push asf-gerrit HEAD:refs/drafts/master`
  - If this command fails, abort and report the first actionable error.
- [ ] Step 9: Pop stash if one was created in Step 3.
  - If Step 3 recorded that a stash was created, run:
    - `git stash pop`
  - If `git stash pop` exits non-zero, report the error and stop; do not abort
    silently (the push already succeeded).
  - If no stash was created in Step 3, skip this step.

## Gotchas

- Gerrit requires `Change-Id:` to be in the final footer paragraph of the commit message.
  Keep all trailer lines contiguous in that last paragraph (for example, `Change-Id:` and
  `Assisted-by:` should be adjacent, with no blank line between them).
- Git may not have `remote.<name>.pushurl` explicitly set. In that case,
  `git remote get-url --push <name>` may still resolve via `remote.<name>.url`.
- `git stash push --keep-index` stashes only unstaged tracked changes and preserves staged
  changes for the next commit step.

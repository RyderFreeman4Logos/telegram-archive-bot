# Release candidate verification

Current target: **0.1.0-rc.5**. This candidate contains the due-recovery fairness
fix (#98) and empty revision-title fallback (#99), with an index-only schema-20
upgrade. The final combined source review, eight-command canonical gate, hosted
CI, Cargo installation, verified-backup activation and public download checks
remain pending at this source freeze. See [RC5 notes](../release-notes-0.1.0-rc.5.md)
and the [versioned release verification assets](https://github.com/RyderFreeman4Logos/telegram-archive-bot/releases/tag/v0.1.0-rc.5)
for the later source-bound outcomes. Historical RC4 receipts do not transfer to
RC5; client/provider tracks #41/#92/#94/#97 remain separate.

## Historical RC4 preparation

The RC4 source snapshot added the daily anonymous X, Notion/PDF, failure-reporting
and external-reader workflow. Its final versioned canonical gate, hosted CI,
Cargo installation, schema-19 activation and published asset verification were
pending at that preparation snapshot. The release verification assets bind their
actual source commit/tree and executable hashes.

Independent leaf gates and real anonymous timeline/content, Notion text/PDF and
synthetic GitHub reporting checks passed with separate source-bound receipts.
The PDF text-normalization failure and interrupted reconciliation attempt remain
recorded alongside successful recovery. External reading must verify full retained
bodies and preserve collection/context gaps; it cannot infer complete analysis
from a title, file, publication status or available fragment.

RC3 was published and deployed, and its main-corpus integrity, Git export and
Git-only restore receipts are historical evidence, not fresh RC4 corpus reruns.
Private watchlists, credentials, content and draft Git history are excluded from
public assets. See [RC4 notes](../release-notes-0.1.0-rc.4.md),
[reader handoffs](../reader-handoffs.md) and [legacy parity](../legacy-parity.md).
Client-access limits in #41 and optional F21/F22 remain explicitly unverified.

Project-Spec: `rust-telegram-archive-bot-v1`. Target: `0.1.0-rc.1`.
The checklist below is the historical RC1 source ledger. RC1 publication is
verified by the release assets; unchecked client controls remain tracked in #41.

## Remaining work

- [x] Diagnose the hosted ADK timeout: debug-bridge hashing exhausted the fixture
  budget before any model POST. Release-bridge tests pass; failed receipts remain.
- [x] Merge PRs #39/#40 after complete hosted gates
  [40017817089](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/40017817089)
  and [40018465385](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/40018465385) passed.
- [x] Audit every F01–F20 criterion and production caller in
  [feature acceptance](feature-acceptance.md); retain F15 A6 in #41.
- [x] Reconcile all F01–F19 issue states after merged evidence; the separately
  tracked F15 client residual remains in #41. The cache correction #43 is closed;
  F20 remains until candidate publication is verified.
- [x] Reproduce and fix unpinned TTS cache identity (#43); retain the failing
  real-audio regression and verify both same-job recovery and pinned sharing.
- [x] Complete product scenarios and the [G01–G12 evidence matrix](f20.md).
- [x] Verify both explicitly opt-in acceptance examples, including refusal checks.
- [x] Inspect actual model/hymt/TTS/audio/Git output and record artifact evidence.
- [x] Exercise live X fallback and yt-dlp; record the missing official X credential.
- [x] Send the authorized test-chat narration and ordinary tracks below/above
  20 minutes; record returned Telegram types and measured/returned durations.
- [ ] Test seeking and speed controls on available Desktop/Android clients;
  record exact versions and any unavailable client access without claiming a pass.
- [x] Record both Rust license inventories, external tool versions, historical
  clean-clone receipt, capabilities, limits and rollback guidance.
- [x] Review source and pass both workspaces' complete local gate at `42b2eef`:
  formatting, Clippy, 137 host tests, one bridge test and both release builds.
- [x] Reinstall that exact public candidate using Cargo and verify the effective
  PATH binaries, hashes, installed-binary acceptance and retained-state replay.
- [x] Complete [hosted gate 40022169182](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/40022169182)
  and merge F20 at `fe8a4d5`, whose full source tree matches installed `42b2eef`.
  Later receipt-only revisions require a separate binary-input comparison.
- [ ] Publish the documented prerelease with binaries/checksums/license evidence,
  reconcile issue states, and verify the published assets and clean checkout.

This source checklist records the candidate before its final gate and publication.
The [release](https://github.com/RyderFreeman4Logos/telegram-archive-bot/releases/tag/v0.1.0-rc.1)
will carry the final source-bound gate, installation and publication receipts.

## Scope and operating constraints

F21/F22 are optional backlog, not required for this candidate. Drafts remain
read-only; no private configuration, archive data or inherited Git history enters
this repository. No production Telegram poller is activated by acceptance tests.

Long local tasks use `nohup` and one shared `wd` relay. Resolve the active
provider's TTL at every arm, retain exact task identities and durable progress,
and reconcile completion before repeating work. Continue this rule after context
compaction. Completion requires evidence, not a lower open-issue count.

## Evidence available before final acceptance

- Notion reading manifest: all 33 specification pages, with stable F01–F22 IDs.
- F01–F19 implementation PRs merged after their hosted gates passed.
- F19 local gate: 128 host tests and one bridge test passed (historical candidate).
- A clean public clone at `a844f7a` installed both Cargo binaries and passed the
  offline replay, backup/restore, immutable-state and service-file checks.
- One installed-release real model/hymt/TTS replay at `a844f7a` completed in
  89.305 seconds with one completed job and two pending deliveries. This is not
  a final-candidate receipt. Subsequent real Telegram document/voice/audio sends
  are confirmed in [F20 evidence](f20.md); client playback remains blocked.

Each historical receipt keeps its source revision; it does not transfer to the
final candidate after changes. Failed hosted runs remain failed evidence.

Final source candidate `42b2eef` passed the complete local and hosted gates with
137 host tests and one bridge test. A fresh public-clone Cargo installation and
installed-command acceptance passed; the retained real test state also repeated
without changing any of its 36 tables. See the [final candidate receipt](f20.md#final-candidate-and-installed-executable).

At `852a24b`, both format and all-target Clippy checks, **136 host tests and
one bridge test**, and both release builds exited 0 in the complete local gate.
The host release build took 4 minutes 41 seconds. The source audit then added an
explicit host-PID observation/reap assertion for a namespaced media descendant;
the TTS identity fix and final test changes require fresh complete gates.

All 33 original Notion exports were rehashed and their public rendering compared
exactly: only titles, date markup and local-link rendering differ. The reading
manifest now labels the original export hash separately from the public file hash;
no specification or draft content was changed.

Client playback and official-X access remain externally blocked, with exact
prerequisites recorded in `capabilities.json` and #41. A candidate can be
published with these pending; the production-validation gate remains open.

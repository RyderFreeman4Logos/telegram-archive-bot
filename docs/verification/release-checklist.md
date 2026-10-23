# Release candidate checklist

Project-Spec: `rust-telegram-archive-bot-v1`. Target: `0.1.0-rc.1`.
This is the active acceptance ledger, not a production-validation claim.

## Remaining work

- [x] Diagnose the hosted ADK timeout: debug-bridge hashing exhausted the fixture
  budget before any model POST. Release-bridge tests pass; failed receipts remain.
- [ ] Merge PRs #39/#40 only after their complete hosted gates pass.
- [x] Audit every F01–F20 criterion and production caller in
  [feature acceptance](feature-acceptance.md); retain F15 A6 in #41.
- [ ] Close satisfied issues after the corresponding implementation reaches main.
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
- [ ] Review the final change and pass formatting, lint, tests and release builds
  for both Rust workspaces on the final candidate.
- [ ] Merge F20, reinstall the final public revision using Cargo, and verify the
  effective PATH binaries, hashes and installed-binary acceptance.
- [ ] Publish the documented prerelease with binaries/checksums/license evidence,
  reconcile issue states, and verify the published assets and clean checkout.

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
- F01–F17 implementation PRs merged; F18/F19 candidates remain under validation.
- F19 local gate: 128 host tests and one bridge test passed (historical candidate).
- A clean public clone at `a844f7a` installed both Cargo binaries and passed the
  offline replay, backup/restore, immutable-state and service-file checks.
- One installed-release real model/hymt/TTS replay at `a844f7a` completed in
  89.305 seconds with one completed job and two pending deliveries. This is not
  a final-candidate receipt. Subsequent real Telegram document/voice/audio sends
  are confirmed in [F20 evidence](f20.md); client playback remains blocked.

Each historical receipt keeps its source revision; it does not transfer to the
final candidate after changes. Failed hosted runs remain failed evidence.

Client playback and official-X access remain externally blocked, with exact
prerequisites recorded in `capabilities.json` and #41. A candidate can be
published with these pending; the production-validation gate remains open.

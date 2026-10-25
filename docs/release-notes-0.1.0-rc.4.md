# 0.1.0-rc.4

This candidate adds daily X collection through an anonymous FxTwitter provider,
original-content hydration, durable Notion publication, best-effort PDF
attachments, deduplicated GitHub failure reporting, and versioned daily manifests
for external readers. It retains the RC3 bot's historical-message/file reply
commands and canonical SQLite/CAS/Git storage.

Daily collection is opt-in. Notion and reader support are built and enabled by
default; missing destination or credentials are explicit pending configuration.
The prepared daily profile uses a private account seed and a Notion archive root;
those private inputs are excluded from this release. No X login or cookies are
required for the anonymous provider. Provider pagination budgets, unknown repost
times, private/missing sources and unavailable media remain explicit gaps.

## Evidence and limits

The anonymous provider was exercised through the real Rust adapter across two
pages and a reopened store. One real retained activity was hydrated into a
285-byte source revision with a verified 5,261-byte raw response, then materialized
without model/TTS/delivery work. This bounded proof does not establish exhaustive
account history, and the retained timeline/media gaps remain partial.

The real Notion service published and independently read back a 294,000-byte
synthetic document. A separate supervised worker published a 3,090-byte synthetic
document and retained its queue across restart. PDF acceptance exposed Notion's
removal of U+000C; the original failure is retained under #87 rather than reported
as success. A corrected synthetic 636-byte PDF passed independent text decoding,
download hash verification and restart reconciliation without duplicate upload.
Final reader and deployment evidence belongs to the release verification assets.

The actual configured GitHub worker created one intentional synthetic issue,
updated its failure count, and reported recovery in that same issue. Independent
readback verified redaction. Four unique events remained after a duplicate retry
and service restart; the synthetic test issue #88 is closed. No production failure
was asserted by this test.

Daily handoffs separate collection coverage, available body/context, verified
publication and external analysis. External agents own the L1–L5 workflow; the
collector does not claim that a person or model read a title, preview or filename.
Portable exports are bounded by actual rendered bytes and retain every selected
immutable source revision. Same-second updates use a source-observation sequence
that survives semantic Git restore.

The completed historical bulk replay covers 22,500 distinct inputs from 6,761
unchanged draft files. Main-corpus integrity, full Git export and Git-only restore
have separate retained receipts. They are historical source-bound results, not
fresh RC4 corpus reruns. See the release verification assets for their identities
and the fresh exact-source gates, installation, activation and download evidence.

## Upgrade and rollback

Schema 19 applies migrations 013–019 in order. Back up and verify the full state
before upgrading; semantic Git export does not back up binary/raw bytes or
operational queues. Preserve the matching old executable and configuration.
Never run the older executable against the migrated database. After new work has
been admitted, reconcile it before restoring old state; rollback must not discard
post-upgrade data.

The host binary version is 0.1.0-rc.4. The unchanged pinned hymt bridge remains
0.1.0-rc.1. Linux packages target Intel Alder Lake on Debian 12/glibc 2.36;
use Cargo source installation on other CPUs.

Desktop/Android seek and speed controls remain unverified in #41, and an actual
historical-reply client roundtrip remains unobserved. One selected private YouTube
video lacks owner-authorized access. Historical RC1 live model/hymt/TTS/audio
receipts retain their original source identity. Live remote Git push,
local/multispeaker TTS, physical power loss and human semantic/audible quality are
untested. F21/F22 remain optional. These limits are not converted into passing
production-validation claims.

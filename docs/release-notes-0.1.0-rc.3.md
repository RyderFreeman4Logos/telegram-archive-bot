# 0.1.0-rc.3

This candidate includes the [RC2 features](release-notes-0.1.0-rc.2.md): legacy
bot parity, historical-message/file replies with `/translate`, `/tts` and
`/summarize`, retained-result resend, and durable SQLite/CAS storage. It adds
bounded Git index updates within each knowledge event. Dependencies, schema 12,
conflict checks, event commit boundaries and recovery rules remain unchanged.

[Issue #64](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/64)
reduces repeated full-index writes. On a synthetic 100,000-file index, three
alternating pairs of four public CLI event groups used 40 index-update calls
before the change and eight after it. The six resulting trees and index listings
matched. Measured time was 2.913–2.964 seconds before and 2.011–2.061 seconds
after. These bounded observations under concurrent load do not establish
whole-corpus throughput. See [storage measurements](verification/storage-performance.md).

## Bulk replay and evidence

The read-only inventory covers 6,761 draft files. Reconciliation includes 22,482
submitted HTTP candidates and 18 additional invalid-only candidates: 22,500
distinct inputs. Latest outcomes are 14,227 complete, 7,528 partial, 681 failed
and 64 rejected. A completed job or retained body does not establish human-reviewed
semantic completeness. Original attempts remain retained. Bulk capture disables
Telegram delivery, model generation, translation, narration and remote Git push.

At this source snapshot, the pinned full main-corpus Git export and measured
Git-only restore are still running or pending. Final RC3 installation, activation,
publication and downloaded-asset verification require fresh receipts. The prior
RC2 installation and running service were attested separately. Historical corpus,
provider and RC2 receipts retain their actual executable/source identities;
they are not fresh RC3 test results. Release verification assets must bind the
final source, gates, review, installed/runtime binaries and sanitized accounting.

## Installation and limits

The host version is 0.1.0-rc.3. The unchanged pinned hymt bridge remains 0.1.0-rc.1.
The Linux binary package targets Intel Alder Lake on Debian 12/glibc 2.36;
use Cargo source installation on other CPUs. Follow [operations](operations.md),
verify a full backup before activation, and retain the matching rollback binary.
Git projection does not back up raw/binary bytes or operational queues.

Desktop/Android seeking and speed controls remain unverified in
[issue #41](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/41).
The actual historical-reply client roundtrip remains unobserved. Official X API
acceptance lacks its bearer credential; one selected YouTube video requires
owner-authorized private-video access. RC1 real-provider/audio receipts remain
historical evidence. Live remote Git push, local/multispeaker TTS, physical power
loss and human semantic/audible quality are untested. F21/F22 remain optional.

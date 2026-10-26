# 0.1.0-rc.5

This candidate fixes two bounded Notion publication defects (DA06 / #73,
DA05 / #72). It is not yet a published or activated release.

- [#98](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/98):
  due recovery work is admitted by immutable creation time and ID. Fresh queue
  rows with `available_at=0` cannot starve an older due reconciliation. Future
  backoff, uncertain-write reconciliation and duplicate-create protections remain.
- [#99](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/99):
  absent or empty revision titles use `Source revision <revision ID>` as the
  publication title. Nonempty titles, including whitespace, and canonical metadata
  and body bytes stay unchanged. Known no-dispatch conflicts require an explicit
  `notion-retry`; uncertain remote writes cannot be re-armed as new creates.

Schema 20 adds only an active-queue creation-order index. It preserves canonical
content, publication identities, attempts, progress and existing conflict states.
It does not automatically retry or republish historical conflicts. External
package dependencies and the pinned hymt bridge remain unchanged. The host
version is 0.1.0-rc.5; the bridge remains 0.1.0-rc.1.

Before upgrading, create and verify a complete state backup and preserve the old
binary and configuration. Semantic Git export does not include raw/binary bytes
or operational queues. A schema-19 binary must not open schema-20 state. Rollback
requires the verified compatible backup and must account for any work admitted
after the upgrade rather than silently discarding it.

## Checks and remaining limits

The #98 leaf has focused offline regression, migration and query-plan evidence.
That evidence is not a combined RC5 canonical gate or installed-runtime result.
The final eight-command gate, independent combined review, Cargo installation,
activation, real publication acceptance and public download verification remain
pending at this source freeze. The [versioned release verification assets](https://github.com/RyderFreeman4Logos/telegram-archive-bot/releases/tag/v0.1.0-rc.5)
record the later actual outcomes without changing this frozen source. Historical
RC4 corpus, provider, PDF and L1–L5 reader receipts retain
their original source/runtime identities; they are not fresh RC5 reruns.

The remaining tracks [#41](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/41),
[#92](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/92),
[#94](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/94) and
[#97](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/97) remain
separate. This candidate does not claim those acceptance or provider gaps are
resolved. F21 search and F22 legacy import remain optional.

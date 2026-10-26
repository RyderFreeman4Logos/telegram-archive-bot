# 0.1.0-rc.5

This candidate improves durable Notion publication (DA06 / #73, DA05 / #72).
Its final source has not yet been published or activated at this source freeze.

- [#98](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/98) /
  [#102](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/102):
  immediate work and due delayed work take alternating admission turns. A small
  destination-scoped SQLite cursor preserves the next turn across restarts.
  Each group uses the existing ordered queue index. Both a backlog of zero-valued
  availability times and repeated slow reconciliations can make progress.
  This bounds admission turns, not remote request duration or an execution SLA.
  Future backoff, recovery holds, create limits and uncertain-write protections
  remain unchanged. Empty or held queues do not advance the cursor.
- [#99](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/99):
  absent or empty revision titles use `Source revision <revision ID>` as the
  record title. The Notion page title remains its stable publication marker.
  Nonempty titles, including whitespace, and canonical source metadata and body
  bytes stay unchanged. Known no-dispatch conflicts require an explicit
  `notion-retry`; uncertain remote writes cannot be re-armed as new creates.

- [#101](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/101):
  refused Notion limits emit fixed reason/scope tags and numeric counts to the
  local structured log. Queries, bodies, identifiers, paths and credentials are
  excluded. Error classification, budgets and retry behavior remain unchanged.
  Journal retention is separate from durable failure events and state backups;
  these diagnostics do not by themselves resolve an unknown remote outcome.

Schema 21 adds an initially empty operational admission cursor and removes the
intermediate schema-20 creation-order index. Existing table contents, publication
identities, attempts, progress and conflicts remain intact. The migration does
not retry or republish historical conflicts. Package dependencies and the pinned
hymt bridge remain unchanged. The host is 0.1.0-rc.5; the bridge is 0.1.0-rc.1.

Before upgrading, create and verify a complete state backup and preserve the old
binary and configuration. Semantic Git export does not include raw/binary bytes
or operational queues. A schema-19 or schema-20 binary must not open schema-21 state. Rollback
requires the verified compatible backup and must account for any work admitted
after the upgrade rather than silently discarding it.

## Checks and remaining limits

The individual changes have focused regression, migration, query-plan and
private-log checks. These do not replace the final combined eight-command gate,
independent review, hosted CI, installation, activation or real recovery checks.
Those and actual public download verification remain pending at this source
freeze. The [versioned release verification assets](https://github.com/RyderFreeman4Logos/telegram-archive-bot/releases/tag/v0.1.0-rc.5)
record subsequent outcomes against the exact source and executable identities.

Historical RC4 and earlier schema-20 RC5 corpus, provider, audio, PDF, reader,
gate and partial recovery receipts retain their original identities. None are
fresh results for this final candidate. An unresolved Notion page is never
recreated merely because a bounded search did not find it.

The remaining tracks [#41](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/41),
[#92](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/92),
[#94](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/94) and
[#97](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/97) remain
separate. This candidate does not claim those acceptance or provider gaps are
resolved. F21 search and F22 legacy import remain optional.

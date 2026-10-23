# Rust Telegram Archive Bot — Greenfield Build Specification

## Purpose
Build a new, public, independently deployable Telegram archive and audio bot with its own Rust workspace and fresh Git history. This is an implementation specification for an agent that can read Notion through `ntn`, create a GitHub repository, open feature issues, and implement the first release.
**Owner:** RyderFreeman4Logos<br>**Specification version:** 1.0<br>**Evidence baseline:** 2026-10-22<br>**Working repository name:** `telegram-archive-bot` (a proposed name, subject to availability).<br>**Delivery status:** Specification and issue definitions; a new repository and implementation have not been created by the author of this document.
## User requirements
- Use an entirely new open-source repository. Keep the existing `drafts` repository outside the implementation scope.
- Implement the bot and its application logic in Rust. Use ADK-Rust for LLM workflows and integrate hymt, yt-dlp, and md2audio through explicit adapters.
- Deliver audio with valid, verified duration and seek behavior. Make narration delivery mode explicit so client playback-speed controls can be evaluated.
- Store a source once per meaningful revision. If B quotes or replies to previously archived A, store the relationship and reuse A rather than embedding another canonical copy.
- Maintain a file representation that can be committed and pushed to Git.
- Provide concrete feature issues and a complete agent handoff.
## Recommended implementation decisions
These are explicit engineering defaults proposed by this specification, not a claim that the user previously chose every detail. Proceed with them unless later user instructions supersede them; record a reasoned architectural decision if implementation evidence requires a change.
- One Rust service and workspace, with durable workers and bounded external processes.
- SQLite for canonical entities, revisions, relations, views, jobs, and outbox records; an immutable content-addressed file store for large blobs. Together they form the authoritative dataset.
- Deterministic, one-way Git projection into a separately configured user archive repository. The new public code repository contains application code, documentation, and synthetic fixtures.
- Optional search indexes are rebuildable. LanceDB is an optional later feature, not a prerequisite for deduplication.
- ADK coordinates typed LLM tasks; host Rust code owns persistence and external side effects. hymt handles translation through its existing library boundary.
- Spoken narration defaults to a voice-message profile; ordinary audio-track delivery remains configurable. Byte correctness and client UI behavior have separate acceptance checks.
- MIT OR Apache-2.0 is the proposed license for newly authored application code, subject to a dependency and copied-code license review.
## Completion contract
The first release is complete only after all core feature issues F01–F20 satisfy their acceptance criteria and the release gates have recorded evidence. F21 search and F22 local legacy import are optional later work.
The two decisive regression scenarios are:
1. Archive A, then B quoting A, then C replying to A. One canonical A revision exists; both relationships and each view's exact revision choices remain reconstructable.
2. Produce narration and downloaded audio, finalize and validate the actual bytes, deliver them, and record the returned media metadata. Upload failure retains the artifact. A supported client can seek the media and the chosen narration mode exposes the expected playback controls.
A crash after accepting an input must leave recoverable work. A crash during an external send may leave an explicitly uncertain delivery; the system must not promise exactly-once Telegram delivery.
## How to use this specification
Read the Agent Handoff first, then the Product Scope and Architecture pages. Follow the remaining specification pages and all 22 children under Feature Backlog. Child pages are real Notion pages, not merely headings. The completed tree contains **33 pages: this root, 10 direct children, and 22 feature pages**.
The feature pages are ready-to-transfer issue definitions with stable Spec-IDs, dependencies, implementation scope, and testable acceptance criteria. The implementation agent should create GitHub issues idempotently, resolve Spec-IDs to issue numbers, and work in dependency order.
## Scope authority
This specification supersedes the previous proposal to patch the old bot before a rewrite. There is no prerequisite to modify the old repository. Legacy importing, if later enabled, is a read-only, explicitly configured local operation.
Do not publish old repository history, personal archives, live credentials, signed media URLs, cookies, private endpoints, or deployment-specific identifiers. Use newly authored code and synthetic fixtures. Public issue text should describe the new product without copying private operational notes.
## Navigation
Start with [00 — Agent Handoff & Repository Bootstrap](00.md). The complete dependency manifest and 22 issue definitions are in [09 — Feature Backlog (22 Issue Definitions)](09.md). The native child-page blocks below preserve the full hierarchy for recursive reading.
[00 — Agent Handoff & Repository Bootstrap](00.md)
[01 — Product Scope & Acceptance Contract](01.md)
[02 — Architecture & Runtime Contracts](02.md)
[03 — Canonical Storage & Git Projection](03.md)
[04 — ADK, hymt & Media Integrations](04.md)
[05 — Audio Finalization & Telegram Delivery](05.md)
[06 — Configuration, Operations & Recovery](06.md)
[07 — Verification & Release Gates](07.md)
[08 — Evidence & Decision Log](08.md)
[09 — Feature Backlog (22 Issue Definitions)](09.md)

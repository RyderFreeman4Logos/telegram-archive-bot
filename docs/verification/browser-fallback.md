# Browser fallback evidence

Project-Spec: rust-telegram-archive-bot-v1. Spec-ID: F04, F09, F20.
Tracking: [#46](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/46).

The initial corpus audit found an HTTP-200 verification page accepted as full
text. The new regression first failed because that interstitial became article
content. Verification, login and Chromium error fixtures now retain raw bytes
without a text revision. A substantive article discussing verification remains
readable. Empty HTTP bodies retain truthful status and bytes; local attachments
do not invent an HTTP status.

Focused checks passed on 2026-10-24: six document tests, the browser namespace
argument test and the rendered-result selection/provenance test. The latter
checks successful DOM selection, both raw objects, unavailable rendered text,
missing tools and cancellation. Clippy passed with warnings denied.

The explicitly enabled installed-browser test ran native Chrome 152.0.7977.64
through the actual application relay and Bubblewrap. JavaScript filled an empty
DOM element; the returned DOM contained that text, and the private workspace
was removed. The final observation took 4.33 seconds. It used a synthetic data
URL, so it does not establish public-network reachability or corpus coverage.
Normal offline gates skip this explicitly opt-in test; a skipped test is not a
live-browser pass.

The first installed-browser attempt failed while creating a storage workspace.
A diagnostic retry passed without a production change; the exact cause was not
established. The fixture was then placed under the project-owned target directory
and passed again. Both failures and successes remain in private local logs.
Earlier document assertions also required a fixture-only trailing-whitespace
correction; those failed logs were retained rather than reported as passes.

Full exact-candidate gates, independent source review, installed-binary checks,
runtime activation and real affected-source reruns have separate receipts. This
document does not assert those later outcomes or universal challenge detection.

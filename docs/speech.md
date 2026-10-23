# Speech producer (F13, issue #13)

`Md2Audio::synthesize` takes a pinned source revision and a CAS UTF-8 input
explicitly identified as original, translation or summary. Original inputs must
match the canonical body. The persisted spoken-text derivative includes the
input hash, revision, full settings, ordered turn bodies and resolved voices.
Universal newlines are normalized like Python's UTF-8 text reader. Everything
else in a single-speaker input, including code, tables, links and references,
is retained with `--no-strip`. This conservative policy may read punctuation;
it makes no claim about the provider pronouncing every token.

A nonempty `[synthesis.speakers]` explicitly requests dialogue. Accepted labels
are line-start `**Name:** text` or their full-width-colon equivalent; nonblank
preambles, unknown labels and empty turns fail before synthesis. Labels are
metadata. Each turn is invoked separately with an exact TOML mapping in the
upstream `[speakers.Name] voice = '...'` schema, `--multi-speaker`, `--no-cache`
and `--no-strip`. This preserves ordered raw segments instead of relying on
upstream MP3 byte concatenation and estimated turn offsets. The seed remains
in the persisted host identity; it is not passed alongside explicit TOML since
upstream rejects that combination. There is no automatic voice lottery.

Local mode must be enabled explicitly, uses a numeric voice ID (Melo 0,
AISHELL3 0–173), and cannot use dialogue. Local model, volume and thread count
are passed only in local mode; failure never falls back to Edge. Local runtime
and model data live under the private state `cache/md2audio-data`, not the
operator's home. The default Edge voice must be changed to `"0"` when enabling
Melo. No pitch flag exists at the audited CLI boundary; nonzero pitch is
rejected. Language, voice, speed, seed, volume, subtitle request, tool snapshot
and input identity participate in the spoken plan and candidate derivations.

Every invocation has a unique mode-0700 workspace, mode-0600 inputs/TOML,
private HOME/cache, a copied snapshotted FFmpeg executable on its private PATH,
no inherited credentials, and the shared process supervisor's byte/time/log
limits and process-group cleanup. A single deadline covers all turns.
Bounded redacted tool failure reports are stored as private diagnostic
derivatives while the lease still permits publication. A dead lease cannot
publish diagnostics or select output.

Successful audio and optional SRT remain raw CAS objects. SRT times are local
to each turn and explicitly unvalidated. F14 must decode each segment and
validate cues before computing cumulative offsets; F15 may send only finalized
artifacts. No raw candidate enters `artifacts`. Completed phase reuse and
validated shared caching are integrated by F18/F19; a delivery retry must never
invoke this producer.

The audited source is [md2audio at 94d2d1d](https://github.com/RyderFreeman4Logos/md2audio/blob/94d2d1d8b2968ea10f891cceb48b7d68175d0843/md2audio.py).
The actual installed `md2audio synth --help` was checked as well. These checks
do not prove current Edge availability, local-model readiness or audible quality.

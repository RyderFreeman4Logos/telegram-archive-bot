# Media download (F12)

`MediaDownloader` inspects a URL with yt-dlp `--dump-single-json --simulate`, then
downloads selected items with `--no-simulate`. The host parses structured JSON;
human stderr wording is never a success signal. Each download prints exactly one
JSON-escaped `after_move:%(filepath)j` result after post-processing. The host
accepts that path only after a successful process exit, verifies its confinement,
regular-file/link status and size, then hashes and retains the exact bytes.

Sources must explicitly enable `media_enabled`. Linux Bubblewrap is required at
the absolute `tools.bubblewrap` path; the initial local checks used version 0.8.0.
The application itself supplies its absolute relay executable path. Missing or
failing isolation never falls back to running yt-dlp on the host network.
`--unshare-all` establishes PID and network namespaces before a private `/proc`;
the host filesystem is mounted read-only, the job workspace is writable, and a
short private Unix-socket directory connects to the host egress proxy. This is a
network boundary for trusted installed tools, not a claim that all readable host
files or filesystem Unix sockets are hidden from a malicious executable.

An internal relay opens an HTTP proxy listener on the isolated loopback and
forwards it over the Unix socket. The host proxy accepts HTTP GET/HEAD/POST and
HTTPS CONNECT, validates URLs and **all** resolved addresses, rejects private,
special and metadata ranges, and connects only to the checked numeric addresses.
Each new connection, including a redirect follow-up, receives the same checks.
Proxy credentials and untrusted Host headers are not forwarded. Connections,
headers, forwarded byte budget and time are bounded. The namespace has no host
network route; a downloader that ignores its proxy cannot open host TCP sockets.
The namespace policy does not rely solely on environment variables or a CLI
proxy flag. No general agent-selected executable or user CLI switch is accepted.

The environment is cleared by the existing process supervisor. yt-dlp ignores
configuration, plugins, cookies, remote components and automatic JavaScript
runtimes. Native downloading is requested; installed FFmpeg performs audio
extraction afterward. Sites needing JavaScript runtimes, cookies or additional
extractor support can fail explicitly; this version does not claim universal
site support. Network, fragment, extractor and file-access retries are zero.
Metadata and every item share one configured download timeout, and a full budget
must fit the persisted job deadline. Existing process-group supervision owns
cancellation, escalation and reap. Filesystem limits remain the F07 sampled
workspace limit plus per-file OS limit, not a whole-host disk or memory quota.

The default forbids accepting playlist metadata. An explicit positive playlist
bound, no greater than the graph-node limit, permits a bounded metadata request.
The host rejects missing/oversized/nested entries before downloading and downloads
each selected item individually with `--no-playlist`. It does not treat a special
nonzero yt-dlp exit as successful truncation of a playlist. Unexpected result
counts, failed post-processing and paths outside the workspace remain failures.

Candidate manifests record source revision, configuration identity, selected
extractor/ID/title, advisory source duration, CAS digest, byte length, producer
executable hash, selected tool versions, final filename, timestamp and order.
Signed media URLs, cookies, headers and full extractor metadata are not copied
into these manifests. A separate bounded stdout limit admits metadata up to the
configured input limit while stderr retains its smaller diagnostic limit.

Every candidate has `pending_finalization: true`. F12 never inserts a ready audio
artifact and source duration remains advisory. A later playlist item can fail
after earlier pending candidates were retained; this does not complete the job
or authorize sending them. The shared F14 finalizer must decode, measure and
validate every candidate before cache reuse or Telegram delivery. Full daemon
dispatch and live site acceptance remain F19/F20 work.

The five integrated checks use a synthetic yt-dlp executable inside **real**
Bubblewrap and the real Rust relay: metadata-only behavior, changed Unicode/quote
filename and extension, altered human diagnostics, failed post-processing after
printed output, unsafe paths, missing/failing wrappers, bounded playlists,
namespace/private-proxy rejection with a working host listener positive control,
and cancellation/timeout with a TERM-resistant descendant. They assert no ready
artifact publication. Existing F07 checks independently assert process-group
descendant death/reap; these adapter fixtures do not independently identify every
inner namespace PID. Two proxy tests check public-address pinning, mixed DNS
rejection, byte bounds, HTTP authority rewriting and private redirect follow-ups.
No live download or playable audio is claimed by these synthetic candidates.

Reference: [yt-dlp 2026.10.21 options and output contract](https://github.com/yt-dlp/yt-dlp/blob/2026.10.21/README.md).

# Web and document extraction (F09)

`Documents` returns a typed extraction for HTML articles, ThreadReader, PDF URLs,
and Telegram text/Markdown/PDF attachments. It does not publish a canonical
revision itself: the job runner owns final fenced selection. Submitted URLs stay
as source identities, including semantic query strings and fragment routes;
resolved URLs and redirects are observations, not authority to merge sources.

`PublicHttp` accepts HTTP(S) without URL credentials, checks every resolved
address at every hop, rejects private/special/metadata destinations, pins DNS
addresses into the client, disables environment proxies, and limits redirects to
five. Credentials cannot cross origins. Both declared and streamed byte sizes
are bounded; one timeout and cancellation cover DNS through the final body byte.
The localhost connector used by the HTTP fixtures exists only in test builds.

HTML uses pinned dom_smoothie 0.18.2 and htmd 0.5.6. ThreadReader's explicit
`.threadreader-content` container has precedence for that provider. General
articles use readability extraction; an explicitly named whole-body fallback is
partial. These are heuristics, not guarantees that a paywalled or dynamic page
was fully available. Short verification/login pages and Chromium error pages
are unavailable text, with their original bytes retained. This is a bounded
heuristic, not a universal challenge detector. Inputs
are bounded by bytes, 50,000 DOM elements and depth 128. CPU parsing runs on the
blocking pool; byte/DOM limits bound it, while cancellation cannot preempt the
Rust parser inside a blocking task. Worker admission must bound concurrent
parses. See the [upstream readability API](https://docs.rs/dom_smoothie/0.18.2/dom_smoothie/struct.Readability.html)
and [HTML conversion API](https://github.com/letmutex/htmd).

A page with scripts and only headings/navigation/links is an unavailable dynamic
shell. Short articles with actual prose remain eligible without a length floor.

With `sources.browser_enabled = true`, a fetched HTML response that is not
complete gets one Chromium headless attempt within the remaining download
deadline. Already complete articles and non-HTML inputs do not launch a browser;
transport failures remain subject to the job's normal retry policy. The DOM
uses the same extraction and interstitial checks. Both initial HTTP bytes and
rendered bytes remain in CAS, with linked evidence. Missing tools, render errors
or another challenge retain the initial capture; cancellation still propagates.

The browser runs in Bubblewrap with separate PID, user and network namespaces,
a bounded temporary profile, cleared environment and selected read-only system
runtime directories. It cannot see the host home, credentials or browser
profile. All network traffic uses the same public-destination proxy policy as
media downloads; implicit loopback proxy bypass is disabled. Browser paths must
resolve under `/usr` or `/opt/google/chrome`. No host network fallback is used.
DOM output, proxy traffic, workspace and execution time are bounded. The job
supervisor owns retries. Browser execution is disabled by default.

Chromium's [headless dump-DOM interface](https://developer.chrome.com/docs/automation-and-testing/headless-cli)
executes page JavaScript but does not report the final HTTP status or resolved
URL. Those rendered observations remain null, while the initial HTTP request's
status/redirects remain separate evidence. For extraction, a rendered HTML base
element takes precedence; otherwise the submitted URL is the explicitly recorded
unverified base for relative links and provider selection. Redirected pages without
a base element can therefore have inaccurate relative links; no final URL is
invented. The renderer uses the documented
[proxy configuration](https://chromium.googlesource.com/chromium/src/+/main/net/docs/proxy.md).
It does not reuse authenticated sessions, solve CAPTCHAs or bypass access controls.

Text decoding honors a BOM, declared HTTP charset, or an HTML meta charset in
the first 1,024 bytes; otherwise UTF-8 is required. Unsupported encodings,
replacement errors and NUL bytes produce an unavailable text result. UTF-8
Markdown is retained without rewriting its syntax. Display filenames discard
path components and control/platform characters, and never become tool paths.

PDF validation runs supervised `pdfinfo`; at least one page must be reported.
The original is then independently marked validated. A separate supervised
`pdftotext -layout -enc UTF-8` creates derived text in a unique private workspace.
Both commands share a deadline. Tool failures, an empty scanned PDF, an absent
or oversized output, and invalid UTF-8 leave the original available and text
unavailable. Successful extraction with diagnostics is partial. This is
structural PDF validation, not sanitization or malware scanning. No OCR is
claimed. An unknown HTTP MIME is rejected unless it is absent and valid UTF-8;
PDF magic is accepted with `application/octet-stream` only.

The result records original digest, media type, sanitized display name,
validation status, text digest, extractor/version, encoding, HTTP status,
retrieval time and bounded tool reports. The canonical content and derived text
never overwrite original bytes. Only the original validation flag permits
later document delivery; unavailable text cannot proceed as a translation.

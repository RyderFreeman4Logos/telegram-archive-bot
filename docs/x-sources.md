# X source graphs

`sources.x_enabled` enables the official X API and requires an `x_bearer` secret
reference. `sources.x_fallback_enabled` separately opts into the public FxEmbed
API. Both are disabled in the example until configured. The fallback receives
the requested public status ID. Every observation names its provider and retained
raw payload; authentication/access failures, missing objects, explicit tombstones,
outages and partial excerpts remain distinct.

The official adapter requests note text and typed quote/reply/repost references.
FxEmbed API v2 supplies text, parent/quote references and supported attached
article blocks. Plain text, headings, lists, quotes, code and Markdown entities
are retained in normalized article content. Unrendered atomic/media entities or
preview-only articles are partial, with the raw payload retained. Direct article
URLs without a supported status lookup use the web adapter and may require a
provider capability not available anonymously. Fallback repost identity gaps are
explicit; the original post is a separate referenced object.

Each traversal uses a visited set and deterministic relation order. Node, depth,
reply-ancestor, total byte and elapsed-time budgets produce explicit omissions.
The persisted view includes only selected traversal members, exact archived
revisions, relation evidence, omissions and retrieval policy. Useful previously
archived full content survives missing/deleted/preview observations.

Concurrent jobs coordinate by status ID. A bounded 128-entry cache reuses recent
small fetches for 30 seconds; each retained entry is at most 256 KiB. Larger
results serialize by ID but are not retained in memory. Refresh bypasses older
cached results. Request-local visited identities still fetch at most once.

All HTTP hops resolve and validate public addresses, pin those addresses for the
connection, reject private/special addresses, disable implicit proxies and bound
redirects/body/time. Bearer credentials cannot cross origins on a redirect.
Configured model services use a separate endpoint class.

Contract sources: [official X post lookup](https://developer.twitter.com/apitools/api?endpoint=%2F2%2Ftweets%2F%7Bid%7D&method=get),
[FxEmbed API schema](https://github.com/FxEmbed/FxEmbed/blob/main/docs/specs/fxtwitter-openapi.json),
[legacy status field reference](https://github.com/FxEmbed/FxEmbed/wiki/Status-Fetch-API).
Provider availability and account entitlement still need opt-in live checks.

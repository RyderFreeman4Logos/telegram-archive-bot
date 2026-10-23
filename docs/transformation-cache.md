# Transformation identity and reuse

Schema 10 records immutable closed transformation runs, the current eligible
selection, each job's use, and one leased in-flight slot per versioned identity.
Identity includes exact ordered revisions and the complete pinned view for
contextual metadata, prompt/schema versions, configured model name, endpoint and
operator-supplied deployment revision, language, effective provider defaults,
configuration snapshot and executable hashes. Operators must change the deployment
revision when weights or provider defaults change; an HTTP model name cannot
attest remote weights.

Narration records the full spoken Markdown, resolved speaker/voice settings and
producer hash. A source-local narration has no view context; a contextual summary
must include its view. Audio profiles, repair policy, FFmpeg/ffprobe hashes and
limits participate in identity. Raw producer output is never a ready cache entry:
all audio must pass finalization, retained-byte verification and candidate/parent
binding before selection or reuse. Corrupt selected bytes fail visibly.

A Python launcher hash does not identify its installed package or TTS deployment.
`synthesis.deployment_revision` identifies the operator-pinned package,
provider/defaults and local weights. Empty revisions scope narration reuse to
the owning job; declared unchanged deployments allow cross-job coalescing.
Changing that revision invalidates the selected spoken plan and audio. This is
an explicit deployment contract, not remote weight attestation.

Concurrent owners wait without holding a SQLite writer. Lease loss, cancellation
and deadline prevent selection and drain the producer. External providers can
still have accepted work before a lost lease; this is not an exactly-once remote
execution claim. Independent jobs retain their own authorized delivery outboxes.
Upload retry does not invoke any model or producer.

Hymt always performs its own bridge preflight. The application serializes matching
translation calls but does not substitute a host cache for hymt's effective
inference fingerprint. Verified upstream cache entries can avoid subsequent model
generation; unverified or partial results cannot become host hits. A forced job
uses a separate upstream cache namespace. It creates a new run and eligible
selection without mutating old runs or artifacts; resuming that same forced job
may reuse its own completed valid run. Identical output bytes can still share CAS
storage, while provenance and run identities remain separate.

Retention currently keeps every catalogued blob. GC removes only abandoned,
uncatalogued objects after the configured grace period. This protects pending and
unknown deliveries and full backups, at the cost of growing disk usage; operators
must monitor free space. No eviction of canonical history is implied.

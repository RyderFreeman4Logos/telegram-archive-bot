# External tool execution

Linux workers use absolute executables and literal argument arrays. Each attempt
gets a unique `0700` directory under managed state; repeated source filenames do
not collide. The supervisor resolves and hashes the executable at invocation,
clears inherited environment, supplies private HOME/TMP/cache directories and a
minimal system PATH, then adds only adapter-provided variables. Adapters supply
their relevant secret values for log redaction and must not pass secrets in argv.

Stdout/stderr capture is bounded independently. Exceeding the limit stops the
group; diagnostics redact complete secrets and truncated secret suffixes.
Outcomes distinguish missing executable, bad configuration, timeout,
cancellation, log/work limit, unsafe output, I/O and nonzero exit. Reports retain
the resolved executable hash, bounded redacted output, exit code and terminating
signal. Startup tool probes record version output in configuration snapshots.

The child starts a fresh process group. Cancellation/timeout sends TERM, waits
briefly, then sends KILL and waits/reaps. Linux subreaper adoption allows cleanup
of ordinary grandchildren after their parent exits. A drop guard also kills the
group if the future is dropped. Supported trusted tools must retain their process
group; this is process supervision, not a sandbox for arbitrary hostile programs.

RLIMIT_FSIZE bounds each output file. A 50 ms scan bounds accepted aggregate
workspace size and entry count, with a final check after exit; transient growth
between scans can exceed the configured aggregate threshold and is rejected.
Reported output paths must be regular, non-hard-linked files below the managed
directory, without traversal or symlink components. Adapters validate each
reported output before retaining it. Media validity is separately checked by F14.

Source-network access policy is supplied by source/download adapters; clearing
the environment alone does not restrict network access.

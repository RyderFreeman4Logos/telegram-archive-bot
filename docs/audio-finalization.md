# Audio finalization (F14, issue #14)

`AudioFinalizer::finalize` is the only writer of ready `artifacts`. It takes
ordered CAS inputs and parent/producer references independently of either
producer. Requested profiles are `audio_track` (MP3 or AAC/M4A) and `voice_note`
(Ogg/Opus). A filename never establishes the format. Input decoding permits
only local-file protocols and an explicit audio/container demuxer list.

For each file, ffprobe must identify exactly one audio stream, finite positive
numeric metadata when available, and bounded channel/sample-rate values.
FFmpeg performs a complete strict decode (`-xerror -err_detect explode`) into
temporary mono 48 kHz signed 16-bit PCM. The byte count divided by 96 gives the
measured milliseconds, independently of the container's duration/index. The
maximum duration discrepancy is `clamp(measured_ms / 100, 250, 1000)` ms to
accommodate codec delay and rounding. Absent/nonfinite duration fails the
consistency check. Full decode does not prove that the provider supplied all
of the original content; that claim is always false in the validation record.

Repair is explicit: `reject`, `remux` or `transcode`. Already compatible, valid
files retain their exact bytes. Compatible codecs may be remuxed to seekable
local outputs: MP3 writes Xing, M4A uses MP4 fast-start. A remuxed output must
pass the same complete decode and duration check. The transcode policy allows
an explicit last attempt using MP3 96 kbit/s or mono Opus 48 kbit/s. Decoder
errors/metadata inconsistencies from the raw source remain in a private CAS
inspection report, with `source_had_errors`; even repaired output makes no
original-completeness claim. A failed remux may fall through to explicit
transcode; cancellation, deadline and resource errors never do.

Every emitted file must meet the actual configured byte and duration limits.
The quality floors are 64 kbit/s for audio tracks and 16 kbit/s for voice notes;
bit rate uses the probe value or actual bytes/measured duration. An oversized
file fails with a request to split into ordered segments. This release does
not silently lower quality to fit a byte cap. Parents may already be ordered
segments. Segment offsets sum validated sample durations, never text estimates.
Requested SRT must have consecutive cue numbers, valid times, nonempty text,
no overlap, and end within 50 ms of its segment's measured duration. Exported
cue times include the validated cumulative offset.

One shared deadline, supervisor and private workspace cover version probes,
inspection and conversions. Manifests retain SHA-256, exact byte length,
input/parent/raw/producer identity, derivation key, tool versions and binary
hashes, requested/effective profile, real container/codec/MIME/channels/sample
rate, duration, operation, subtitle reference, order and offset. Tool output
and source diagnostics stay private. A single lease-fenced SQLite transaction
registers the whole batch only after every segment passes. Cancellation or a
later failed segment cannot register an earlier partial batch. Schema 6 makes
ready-artifact rows immutable; reads verify their catalog fields and CAS hash.

Fixtures used real FFmpeg/ffprobe 5.1.9 locally. The 3+17 second MP3 fixture
uses different bit rates: stream-copy remux exits successfully while its strict
decode still fails. Explicit re-encoding produces a clean roughly 20-second
output. This is a synthetic mechanism demonstration, not a diagnosis of any
user recording. Probe/decoder receipts do not certify audible completeness,
Telegram seeking or client playback-speed controls.

Container options follow [FFmpeg's muxer documentation](https://ffmpeg.org/ffmpeg-formats.html#mp3).

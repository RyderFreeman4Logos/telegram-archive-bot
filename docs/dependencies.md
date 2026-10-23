# Dependency and license inventory

New application code: MIT OR Apache-2.0. No legacy code is copied or vendored.
Cargo.lock pins the resolved Rust dependency graph. Initial dependencies anyhow,
clap, serde and serde_json use permissive MIT/Apache licensing.

Selected integration baselines, checked against public upstream on 2026-10-23 UTC:

| Component | Baseline | Boundary / license |
| --- | --- | --- |
| ADK-Rust | v2.3.0 | Rust libraries / Apache-2.0 |
| hymt | de670b21b26b3a88a8d711845c9e3d3b9af4c0ba | Rust libraries / Apache-2.0 |
| yt-dlp | 2026.10.21 | External executable / Unlicense source; distributions may include other licenses |
| md2audio | live-tested install: 9795689e9e3787d8324af2678bcc06b0483bf503 | External executable / MIT |
| FFmpeg / ffprobe | operator-provided | External executables; LGPL/GPL depends on build |
| Poppler pdftotext / pdfinfo | operator-provided | External GPL executables |
| dom_smoothie / htmd / encoding_rs | 0.18.2 / 0.5.6 / 0.8.42 | Rust extraction libraries / MIT / Apache-2.0 / MIT OR Apache-2.0 |
| Git | operator-provided | External GPL executable |
| Bubblewrap | operator-provided; local fixture 0.8.0 | External LGPL-2.0-or-later executable; required Linux namespace boundary |

These tools are not bundled or relicensed. Installation must retain each tool's
license and satisfy its distribution requirements. Provider terms and media
rights remain operator responsibilities. Tool versions are recorded by doctor.
Both locked dependency graphs are recorded in the [resolved inventory](verification/dependency-licenses.json)
(452 unique packages, including target-specific and development dependencies).
[Third-party notices](verification/third-party-notices.txt) retain packaged and
pinned-upstream license texts, grouped by identical bytes. Where a crate declares
an Apache-2.0 alternative without a standalone license file, that alternative and
its package attribution are recorded explicitly. MPL-2.0 dependencies are
unmodified and their exact crate sources are supplied as a release attachment;
all source download locations are recorded in the inventory. No external CLI
binary is included. The earlier md2audio integration baseline was `94d2d1d`;
the live installation's `direct_url.json` identifies the tested revision above.

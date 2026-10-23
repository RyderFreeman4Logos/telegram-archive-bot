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
| md2audio | 94d2d1d8b2968ea10f891cceb48b7d68175d0843 | External executable / MIT |
| FFmpeg / ffprobe | operator-provided | External executables; LGPL/GPL depends on build |
| Poppler pdftotext / pdfinfo | operator-provided | External GPL executables |
| Git | operator-provided | External GPL executable |

These tools are not bundled or relicensed. Installation must retain each tool's
license and satisfy its distribution requirements. Provider terms and media
rights remain operator responsibilities. Tool versions are recorded by doctor.
Full resolved dependency inventory is generated before release.

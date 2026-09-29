# MirrorPulse Local Directory Adapter

This is the official repository for the MirrorPulse Local Directory Adapter.

The repository contains the independently buildable Adapter SDK and a Local Directory protocol Worker. The Worker runs in its own process, uses the current-user Named Pipe protocol, confines all paths to the configured source directory, supports bounded range reads, conditional uploads, and transfer-cache staging, and publishes x64/ARM64 `.mpadapter` payloads.

Run `pwsh ./eng/verify.ps1` to validate the SDK and Worker. Releases are produced by the signed workflow after the organization signing secrets and release policy are configured.

Licensed under Apache-2.0. See [LICENSE](LICENSE).

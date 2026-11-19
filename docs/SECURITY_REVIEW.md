# Final security review

Review date is November 18, 2026. The review used the Stage 7 Trivy scan of the validated, digest-pinned Python application image and checked all 11 remaining high-severity CVEs against Debian's security tracker. The platform release uses the same base and dependency pins.

## Debian package findings

The scan contains **47 high-severity package occurrences across 11 distinct CVEs**. Debian still listed Trixie as vulnerable for each CVE on the review date. The scan showed one deferred fix and no vendor-fixed version for these entries.

| CVE | Debian source family | Package occurrences | Recorded disposition |
| --- | --- | ---: | --- |
| [CVE-2025-69720](https://security-tracker.debian.org/tracker/CVE-2025-69720) | ncurses | 4 | Affected |
| [CVE-2026-119373](https://security-tracker.debian.org/tracker/CVE-2026-119373) | SQLite | 1 | Affected |
| [CVE-2026-119374](https://security-tracker.debian.org/tracker/CVE-2026-119374) | SQLite | 1 | Affected |
| [CVE-2026-119379](https://security-tracker.debian.org/tracker/CVE-2026-119379) | SQLite | 1 | Affected |
| [CVE-2026-16742](https://security-tracker.debian.org/tracker/CVE-2026-16742) | systemd | 2 | Affected |
| [CVE-2026-54369](https://security-tracker.debian.org/tracker/CVE-2026-54369) | ACL | 1 | Affected |
| [CVE-2026-76642](https://security-tracker.debian.org/tracker/CVE-2026-76642) | util-linux | 9 | Affected |
| [CVE-2026-78408](https://security-tracker.debian.org/tracker/CVE-2026-78408) | util-linux | 9 | Affected |
| [CVE-2026-78409](https://security-tracker.debian.org/tracker/CVE-2026-78409) | util-linux | 9 | Affected |
| [CVE-2026-78410](https://security-tracker.debian.org/tracker/CVE-2026-78410) | util-linux | 9 | Affected |
| [CVE-2026-9538](https://security-tracker.debian.org/tracker/CVE-2026-9538) | Perl | 1 | Fix deferred |

The existing zlib security package cleared both fixable findings, CVE-2026-85091 and CVE-2026-27171, in the patched image. No additional supported Debian patch was available for the 11 high CVEs. The Stage 7 scan reported zero critical findings and zero Python dependency vulnerabilities. The fixable-findings gate passed, and the unfiltered report retained the outstanding findings. Treat the unfixed highs as a production image risk. Re-evaluate them when Debian publishes fixes or before enabling broad image distribution. [DevSecOps scan evidence and SBOM](DEVSECOPS.md)

## Repository and publication controls

Gitleaks and a tracked-content and history review checked for leaked credentials, private home paths, serialized credentials, state and database files, installed packages, and unrelated project artifacts. The repository contains the original platform implementation, synthetic scenarios, local validation captures, deployment assets and upstream references. The [notices](../THIRD_PARTY_NOTICES.md) record third-party components and assets.

Ignore rules cover lab kubeconfigs, environment secrets, SQLite files, installed dependencies and Terraform state and cache. The Docker build context admits only its Dockerfile, the runtime lock and application source. Live experiments use a dedicated Kind identity. The local monitoring UIs bind to loopback.

GitHub-hosted CI and CodeQL are configured for a future hosted run. The manual GHCR workflow requires an existing stable release, confirmation on the default branch, and the protected release environment. Configure required reviewers on that environment before authorizing image publication. [Publication commands and gates](PUBLICATION.md)

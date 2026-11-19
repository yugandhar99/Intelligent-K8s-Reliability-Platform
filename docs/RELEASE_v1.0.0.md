# Intelligent Kubernetes Maintenance & Reliability Platform v1.0.0

The first portfolio release brings together safe Kubernetes maintenance rehearsal, durable execution, AIOps, and operational telemetry in a repeatable local environment.

## Highlights

- Simulate single-node and multi-node maintenance with capacity, placement, readiness and disruption checks.
- Exercise real eviction and recovery behavior in a dedicated local Kind cluster with explicit approval and verified ownership.
- Correlate infrastructure events into incidents, review evidence and runbooks, and approve allowlisted remediation with an audit trail.
- Inspect reliability through Prometheus, Grafana dashboards, alerting, and OpenTelemetry traces.
- Deploy the simulator with a non-root container, a single-writer Helm chart, and isolated Terraform infrastructure.

## Verification

The local suite passed **265 tests**, including six Kind tests. A burst of 100 concurrent simulator reads delivered **285.05 requests per second**, **320.96 ms P95**, and zero errors. Twenty simultaneous submissions against one plan returned one durable job, which completed successfully. A real four-pod Kind drain completed in **11.82 seconds**, with **24/24 HTTP probes** succeeding across both workloads.

Docker Compose, Helm, Terraform and workflow syntax were validated locally. GitHub-hosted CI and CodeQL execution remain unverified. The base image retains 11 high-severity Debian CVEs with no available vendor fix in the review data.

Use the [README](../README.md) to start the simulator. See the [performance report](PERFORMANCE.md), [security review](SECURITY_REVIEW.md), and [changelog](../CHANGELOG.md) for details.

Original project code is released under the [MIT license](../LICENSE). [Third-party notices](../THIRD_PARTY_NOTICES.md)

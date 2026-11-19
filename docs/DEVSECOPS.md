# Containerization, deployment and DevSecOps

The platform defaults to simulation. The image runs one FastAPI process and its durable maintenance worker together. Compose and Helm use that same image. Infrastructure validation uses only the dedicated local `k8s-reliability-lab` cluster.

## Build and run the complete Compose stack

The multi-stage build installs the SHA-256-hashed runtime dependency lock into a virtual environment and copies the environment and application code into the runtime image. The base image and third-party monitoring services are pinned by digest. The application runs as UID/GID 10001. Compose uses a read-only root filesystem, removes Linux capabilities, enables no-new-privileges, and gives Uvicorn time to stop and release the database.

```bash
export OBS_GRAFANA_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
docker compose -f compose.monitoring.yaml -f compose.platform.yaml up --build -d --wait
curl -fsS http://127.0.0.1:8000/ready
python3 deploy/smoke.py
```

Compose publishes the app and monitoring UIs on loopback. Prometheus scrapes the container over the Compose network, and the application exports traces to Jaeger on that network. To run alongside an existing stack, use a distinct Compose project name and set `PLATFORM_PORT`, `OBS_PROMETHEUS_PORT`, `OBS_GRAFANA_PORT`, `OBS_ALERTMANAGER_PORT`, `OBS_RECEIVER_PORT`, `OBS_JAEGER_PORT`, and `OBS_OTLP_PORT` to unused local ports. Keep the same Grafana password when reusing its volume. The [observability guide](OBSERVABILITY.md) describes the dashboards and alerts.

The named volume `platform-data` contains SQLite and the application lock file. Recreating the app container with the same volume preserves jobs and audit data. `/health` checks application and storage health. `/ready` also requires an active worker heartbeat. The image and chart enable `MAINTENANCE_EXCLUSIVE_DB=1`, which holds a local filesystem lock for the app lifespan. Keep one app process per mounted database and use storage with reliable Linux file locking.

### Dependency locks

Use Python 3.12 and uv 0.13.9 to regenerate the Linux locks. The runtime image uses `requirements.lock`. CI uses `requirements-ci.lock`, which includes test and lint tools and retains the runtime package versions. Existing Python setup via `requirements.txt` continues to work.

```bash
uv pip compile --python-version 3.12 --python-platform linux --generate-hashes \
  --only-binary :all: requirements-runtime.in -o requirements.lock
uv pip compile --python-version 3.12 --python-platform linux --generate-hashes \
  --only-binary :all: -c requirements.lock requirements-ci.in -o requirements-ci.lock
```

## Terraform owns the local namespace

The reusable `lab-namespace` module takes a `stage7-tf-*` namespace name and an environment label. It creates that namespace with restricted Pod Security, a metadata ConfigMap, and an observer identity with `get` on that ConfigMap only. It exports the namespace and observer service account name. The environment examples under `deploy/terraform/environments/local` cover development, testing, and production-style defaults. They all use the local Kind lab.

The local root verifies a single dedicated context, HTTPS loopback endpoint, pinned certificate authority and matching `kube-system` namespace UID. Its provider uses explicitly selected credentials. Run [Kind bootstrap](KUBERNETES.md) to create the ignored local kubeconfig and pin. Keep Terraform provider downloads, plans and state under `data/`.

```bash
root="$PWD"
tf=deploy/terraform/environments/local
mkdir -p data
export TF_DATA_DIR="$root/data/terraform-stage7"
python3 deploy/verify_lab.py
terraform fmt -check -recursive deploy/terraform
terraform -chdir="$tf" init -backend=false -lockfile=readonly
terraform -chdir="$tf" validate
terraform -chdir="$tf" plan -input=false -var-file=testing.tfvars.example \
  -state="$root/data/stage7.tfstate" -out="$root/data/stage7.tfplan"
python3 deploy/verify_lab.py
terraform -chdir="$tf" apply -input=false -state="$root/data/stage7.tfstate" "$root/data/stage7.tfplan"
```

Terraform creates only namespace infrastructure. Helm owns the app Deployment, ClusterIP Service, application ConfigMap, app identities, PVC when provisioned by the chart, PDB, NetworkPolicy and smoke job. Omit `--create-namespace` from Helm commands when using the Terraform namespace. Each release uses its own database.

## Helm uses one application pod

The chart fixes replicas at one and uses `Recreate`. The app claims its database before initialization and holds that lock until shutdown. The pod uses restricted non-root settings, resource bounds, and a readiness probe that checks the worker. Its service account disables token mounting and has no RoleBinding. The optional config-reader identity can read the release ConfigMap by name. It runs independently of the app identity. The chart expects an existing Secret only if `existingOperatorSecret` is set, and reads its `operator-tokens` key.

NetworkPolicy limits app ingress to the release smoke job and explicitly allowed namespaces. It denies egress for the default offline simulation. NetworkPolicy enforcement depends on the cluster CNI. The single-pod PDB protects one healthy pod against voluntary eviction. Expect a short interruption during Recreate updates. The default PVC retention policy keeps data on uninstall. The testing profile uses a removable claim for the isolated lifecycle check.

```bash
chart=deploy/helm/reliability
helm lint "$chart" -f "$chart/values-development.yaml"
helm template stage7 "$chart" --namespace stage7-tf-testing -f "$chart/values-testing.yaml"
kind load docker-image reliability-platform:stage7-local --name k8s-reliability-lab
python3 deploy/helm_lifecycle.py --namespace stage7-tf-testing
```

The lifecycle command verifies lab identity before each Kubernetes command, then installs, smoke-tests, runs a simulation job, upgrades the API SLO, and rolls back. It checks that the pod changes and the same PVC and job survive. It finally uninstalls its tested release and verifies the Terraform metadata remains in place.

For a production-style **local** rendering, select `values-production.yaml` and set `image.repository` and an explicit `image.digest`. Use a separate single-writer filesystem volume for each release. The existing live maintenance workflow remains available from the host through the pinned Kind adapter. Compose and Helm deploy the simulator profile.

## CI, security and release

- **DevSecOps validation** installs hashed Python dependencies, runs pytest, checks deployment formatting, checks Python source with Ruff, and validates GitHub Actions syntax with actionlint. Dedicated jobs lint and render Helm, validate Terraform, and run Kind integration and the Helm lifecycle. Gitleaks scans Git history. pip-audit checks both dependency locks. Trivy scans rendered configuration, Terraform, the Dockerfile and the image.
- **Release evidence** includes a Trivy application image report, CycloneDX SBOM, Helm package and checksums. The image secret check runs before artifact upload. The vulnerability gate blocks fixable HIGH and CRITICAL findings. The full vulnerability report retains findings that currently have no vendor fix.
- **CodeQL** is configured for Python and GitHub Actions. Only its analysis job requests security-event write access. Dependabot covers pip, Docker, and GitHub Actions. Third-party Actions are pinned to commit IDs. Local Linux amd64 tooling can be fetched with `python3 deploy/install_tools.py data/bin …`, which checks stored SHA-256 sums.
- **GHCR** uses a manual dispatch with the exact confirmation `publish`. The protected `ghcr-release` job checks that the selected ref is the default branch and that its commit matches an existing, published stable release tag. Configure required reviewers on the GitHub environment before enabling publication. Package write permission is scoped to that job. Only the authorized version tag is pushed.

## Local validation and findings

| Check | Observed result |
| --- | --- |
| Regression suite | 265 tests passed in 76.95 s under the pinned Python 3.12 CI lock. This preserved all 243 prior tests and added 22 deployment tests. Six tests exercised the dedicated Kind cluster. |
| Compose | The complete stack ran on isolated loopback ports. The app was healthy under UID 10001 with zero effective Linux capabilities and no-new-privileges enabled. A durable job succeeded and remained accessible after container recreation and the patched image upgrade. All three Compose Prometheus targets were up and promtool passed the recording and alert rule fixtures. |
| Terraform | Format, initialization with locked provider checksums, validation and plan passed. The scoped apply added five lab Kubernetes objects and one Terraform guard. The next plan showed no drift. Kubernetes authorization checks allowed the observer to read its ConfigMap and denied secret access and Deployment updates. |
| Helm | Install took 10.72 s, upgrade 7.02 s, rollback 8.46 s, and uninstall 2.39 s in the final lifecycle run. Both Helm smoke jobs passed. The pod was replaced during upgrade and rollback, the SLO was restored, and the same PVC and completed job survived. The Terraform namespace and metadata remained after Helm uninstall. |
| Local CI checks | Ruff, actionlint, chart lint and rendering, and release artifact checksums passed. The Helm package and CycloneDX SBOM were generated locally. |
| Python dependencies and secrets | pip-audit reported zero findings across 39 runtime and 64 CI dependencies. Trivy reported zero secrets in the application image. Gitleaks reported zero findings in Git history and the staged diff. |
| Configuration | Trivy reported zero failures across the two rendered Helm profiles, Terraform and the Dockerfile. |

The application image scan initially found two fixable zlib findings, CVE-2026-85091 and CVE-2026-27171. The rebuilt image pins the Debian security package `zlib1g=1:1.3.dfsg+really1.3.1-1+deb13u1`. Both findings cleared on rescan. The fixable HIGH/CRITICAL gate passed.

The full rescan still reported 224 Debian package findings with unavailable vendor fixes in the scan database. These included 47 HIGH package occurrences representing 11 distinct CVEs, 103 MEDIUM, 67 LOW and seven UNKNOWN occurrences. The high findings affected inherited Debian packages including SQLite, util-linux, ncurses, systemd libraries, ACL and Perl. Zero CRITICAL or Python package vulnerabilities were reported. The gate's result covers fixable findings, and the full report is the record of remaining exposure. The scan covered the built application image. Monitoring service images have their own upstream lifecycle.

The [application SBOM](evidence/stage7-platform.cdx.json) is the actual CycloneDX 1.7 document from the patched local image and lists 127 components.

## Limitations

The Helm deployment deliberately uses a single writer and a local filesystem lock. High availability requires a different storage and coordination design. The lab records Helm and NetworkPolicy objects alongside the restricted namespace configuration. Network enforcement requires an enforcing CNI. The Terraform root is scoped to this pinned Kind cluster with local state.

Hosted workflows are prepared and syntax-checked locally. Their GitHub executions and repository-side environment review settings are separate from the local validation. [GitHub workflow security guidance](https://docs.github.com/en/actions/reference/security/secure-use)

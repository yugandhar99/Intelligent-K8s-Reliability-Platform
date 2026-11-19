# GitHub publication

The prepared repository name is **Intelligent-K8s-Reliability-Platform**. Local release content is ready for review. Run these commands from the project root only after explicit approval to publish the source and create the GitHub release. The commands require the GitHub CLI, an authenticated account with permission to create a public repository, and the repository name to be available for that owner.

```bash
git status --short
gh auth status
release_owner="$(gh api user --jq .login)"
release_repo="$release_owner/Intelligent-K8s-Reliability-Platform"

gh repo create "$release_repo" --public --source=. --remote=origin \
  --description "Safe Kubernetes maintenance rehearsal, durable recovery, and evidence-driven AIOps"
git push -u origin HEAD:main
gh repo edit "$release_repo" --default-branch main
```

Review the GitHub-hosted CI and CodeQL runs after the approved push. After explicit approval to create the release, tag the validated commit and publish its notes.

```bash
git tag -a v1.0.0 -m "Intelligent K8s Reliability Platform v1.0.0"
git push origin refs/tags/v1.0.0
gh release create v1.0.0 --repo "$release_repo" --verify-tag \
  --title "Intelligent Kubernetes Maintenance & Reliability Platform v1.0.0" \
  --notes-file docs/RELEASE_v1.0.0.md
```

GHCR image publication is a separate authorization. Configure required reviewers for the `ghcr-release` GitHub Environment and review the [Debian findings](SECURITY_REVIEW.md) before authorizing the workflow. The workflow uses the published stable tag and an explicit `publish` confirmation. The existing [SBOM](evidence/stage7-platform.cdx.json) documents the validated local image. The CI image job generates evidence for subsequent builds.

[GitHub CLI repository creation](https://cli.github.com/manual/gh_repo_create) · [GitHub CLI release creation](https://cli.github.com/manual/gh_release_create)

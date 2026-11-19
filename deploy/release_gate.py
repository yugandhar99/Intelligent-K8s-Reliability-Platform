"""Check that a manual GHCR dispatch refers to an existing stable GitHub release."""

import json
import os
import re
import subprocess
from urllib.request import Request, urlopen


def verify_release(tag: str, head: str, repository: str, token: str) -> str:
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ValueError("Release tag must be a stable vMAJOR.MINOR.PATCH tag")
    if not re.fullmatch(r"[a-f0-9]{40}", head):
        raise ValueError("Expected full Git commit ID")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Invalid GitHub repository")
    target = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}"],
        text=True,
    ).strip()
    if target != head:
        raise ValueError(
            "The existing release tag must point to the authorized checkout"
        )
    request = Request(
        f"https://api.github.com/repos/{repository}/releases/tags/{tag}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=15) as response:
        release = json.load(response)
    if (
        release["tag_name"] != tag
        or release["draft"]
        or release["prerelease"]
        or not release["published_at"]
    ):
        raise ValueError("Expected an existing published stable GitHub release")
    return f"ghcr.io/{repository.lower()}:{tag}"


if __name__ == "__main__":
    if os.environ.get("RELEASE_CONFIRMATION") != "publish":
        raise SystemExit("Explicit publication confirmation is required")
    print(
        verify_release(
            os.environ["RELEASE_TAG"],
            os.environ["GITHUB_SHA"],
            os.environ["GITHUB_REPOSITORY"],
            os.environ["GH_TOKEN"],
        )
    )

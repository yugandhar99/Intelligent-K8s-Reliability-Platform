"""Install checksum-pinned Linux amd64 release tools into an explicit directory."""

import argparse
import tarfile
import zipfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen

GITHUB = "https://github.com/"
TOOLS = {
    "kind": (
        "https://kind.sigs.k8s.io/dl/v0.31.0/kind-linux-amd64",
        "eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa",
        "",
    ),
    "helm": (
        "https://get.helm.sh/helm-v3.18.6-linux-amd64.tar.gz",
        "3f43c0aa57243852dd542493a0f54f1396c0bc8ec7296bbb2c01e802010819ce",
        "linux-amd64/helm",
    ),
    "terraform": (
        "https://releases.hashicorp.com/terraform/1.13.5/terraform_1.13.5_linux_amd64.zip",
        "0dbe3fcc268eb670801af6a6456799d1ae26e72e73797f6c6167e18aafd1fd9a",
        "terraform",
    ),
    "actionlint": (
        GITHUB
        + "rhysd/actionlint/releases/download/v1.7.7/actionlint_1.7.7_linux_amd64.tar.gz",
        "023070a287cd8cccd71515fedc843f1985bf96c436b7effaecce67290e7e0757",
        "actionlint",
    ),
    "gitleaks": (
        GITHUB
        + "gitleaks/gitleaks/releases/download/v8.28.0/gitleaks_8.28.0_linux_x64.tar.gz",
        "a65b5253807a68ac0cafa4414031fd740aeb55f54fb7e55f386acb52e6a840eb",
        "gitleaks",
    ),
    "trivy": (
        GITHUB
        + "aquasecurity/trivy/releases/download/v0.74.0/trivy_0.74.0_Linux-64bit.tar.gz",
        "2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a",
        "trivy",
    ),
}


def install(destination: Path, name: str) -> None:
    url, digest, member = TOOLS[name]
    with urlopen(url, timeout=120) as response:
        raw = response.read()
    if sha256(raw).hexdigest() != digest:
        raise ValueError(f"Checksum mismatch for {name}")
    if url.endswith(".tar.gz"):
        with tarfile.open(fileobj=BytesIO(raw), mode="r:gz") as archive:
            handle = archive.extractfile(member)
            if handle is None:
                raise ValueError(f"Missing binary for {name}")
            raw = handle.read()
    elif url.endswith(".zip"):
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            raw = archive.read(member)
    path = destination / name
    path.write_bytes(raw)
    path.chmod(0o755)
    print(f"Installed checksum-verified {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("tools", nargs="+", choices=TOOLS)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    for tool in args.tools:
        install(args.directory, tool)

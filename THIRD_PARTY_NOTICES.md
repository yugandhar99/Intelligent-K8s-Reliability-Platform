# Third-party notices

The [MIT license](LICENSE) applies to original project code and documentation. Third-party software is obtained from its upstream distributions and retains its own licensing and notices.

- The [dependency lock](requirements.lock) identifies the Python runtime packages. Installed wheels retain their published license metadata.
- The [CycloneDX SBOM](docs/evidence/stage7-platform.cdx.json) identifies the Python and Debian components in the validated application image. Consult each component's upstream license and the package notices supplied with the image.
- The [monitoring Compose stack](compose.monitoring.yaml) references upstream Prometheus, Grafana, Alertmanager, Jaeger and Python images by digest. License terms remain those of the corresponding image and its components.
- The [Kind manifests](kubernetes/) reference upstream Kind and NGINX images. Helm, Terraform and other validation tools are downloaded from checksum-pinned releases. [Tool versions](deploy/install_tools.py)
- The dashboard PNGs are original captures of this project's local Grafana instance. The Grafana interface and marks shown in those captures belong to their respective owners.

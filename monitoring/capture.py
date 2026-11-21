"""Capture the four provisioned dashboards in a real authenticated browser session."""

import argparse
import os
from pathlib import Path 

from playwright.sync_api import sync_playwright

DASHBOARDS = {
    "cluster-health": "Kubernetes Cluster Health",
    "maintenance-reliability": "Maintenance Operations and Reliability",
    "aiops-performance": "AIOps and AI Provider Performance",
    "slo-error-budget": "SLO and Error Budget Monitoring",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:3000")
    parser.add_argument("--output", type=Path, default=Path("/screenshots"))
    parser.add_argument("--from-time", default="now-10m")
    parser.add_argument("--to-time", default="now")
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1536, "height": 1150}, device_scale_factor=1, locale="en-US", timezone_id="America/Chicago")
        login = page.request.post(f"{args.base}/login", data={
            "user": "admin", "password": os.environ["OBS_GRAFANA_PASSWORD"],
        })
        if not login.ok:
            raise RuntimeError(f"Grafana login returned {login.status}")
        args.output.mkdir(parents=True, exist_ok=True)
        for uid, title in DASHBOARDS.items():
            dashboard = page.request.get(f"{args.base}/api/dashboards/uid/{uid}")
            if not dashboard.ok:
                raise RuntimeError(f"Grafana dashboard {uid} returned {dashboard.status}")
            path = dashboard.json()["meta"]["url"]
            url = f"{args.base}{path}?from={args.from_time}&to={args.to_time}&theme=dark"
            page.goto(url, wait_until="networkidle")
            page.get_by_text(title, exact=True).first.wait_for(timeout=30_000)
            page.wait_for_function("document.querySelectorAll('canvas').length >= 4", timeout=30_000)
            page.wait_for_timeout(900)
            page.screenshot(path=args.output / f"{uid}.png", full_page=True, animations="disabled")
            print(f"Captured {title}", flush=True)
        browser.close()


if __name__ == "__main__":
    main()

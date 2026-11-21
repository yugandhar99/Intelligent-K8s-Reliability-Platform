"""Keep a bounded, local evidence buffer of Alertmanager notifications."""

from collections import deque 
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Lock
from time import time

messages = deque(maxlen=500)
lock = Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/alerts":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 100_000:
                raise ValueError
            alert = json.loads(self.rfile.read(length))
            event = {
                "received_at": time(), "status": alert["status"], "group_key": alert["groupKey"],
                "group_labels": alert["groupLabels"],
                "alerts": [{key: item[key] for key in ("status", "fingerprint", "startsAt", "endsAt")}
                           for item in alert["alerts"]],
            }
        except (ValueError, KeyError, TypeError):
            self.send_error(400)
            return
        with lock:
            messages.append(event)
        print(json.dumps(event), flush=True)
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        if self.path != "/notifications":
            self.send_error(404)
            return
        with lock:
            body = json.dumps(list(messages)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9095), Receiver).serve_forever()

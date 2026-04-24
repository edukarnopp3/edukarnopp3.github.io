from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

if not os.getenv("ISEQ_BEARER_TOKEN") and not os.getenv("ISEQ_EXPORT_DIR"):
    downloads = Path.home() / "Downloads"
    if downloads.exists():
        os.environ["ISEQ_EXPORT_DIR"] = str(downloads)

os.environ.setdefault("ISEQ_STORAGE_DIR", str(Path(__file__).resolve().parent / "storage"))

from app.jobs import JobStore  # noqa: E402


store = JobStore(storage_dir=os.environ["ISEQ_STORAGE_DIR"])


class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
        self.respond(204, None)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/health":
            mode = "api" if os.getenv("ISEQ_BEARER_TOKEN") else "local_files" if os.getenv("ISEQ_EXPORT_DIR") else "not_configured"
            self.respond(200, {"status": "ok", "collector_mode": mode, "collector_dir": os.getenv("ISEQ_EXPORT_DIR", "")})
            return

        if path.startswith("/api/iseq/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4:
                job = store.get_job(parts[3])
                if not job:
                    self.respond(404, {"detail": "Job não encontrado."})
                    return
                self.respond(200, asdict(job))
                return
            if len(parts) == 5 and parts[4] == "data":
                job = store.get_job(parts[3])
                if not job:
                    self.respond(404, {"detail": "Job não encontrado."})
                    return
                if job.status != "completed":
                    self.respond(409, {"detail": "Job ainda não concluído."})
                    return
                self.respond(200, {"rows": store.get_data(parts[3])})
                return

        self.respond(404, {"detail": "Endpoint não encontrado."})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path != "/api/iseq/jobs":
            self.respond(404, {"detail": "Endpoint não encontrado."})
            return

        try:
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length) or "{}")
            start = datetime.fromisoformat(payload["start"])
            end = datetime.fromisoformat(payload["end"])
            if end <= start:
                self.respond(400, {"detail": "A data final deve ser posterior à data inicial."})
                return
            job = store.create_job(payload.get("equipment_id", "1C:69:20:C7:31:D8"), start, end)
            self.respond(200, asdict(job))
        except Exception as exc:
            self.respond(400, {"detail": str(exc)})

    def respond(self, status: int, payload: object | None) -> None:
        body = b"" if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        print(f"{self.address_string()} - {format % args}")


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    print(f"Servidor local ISEQ em http://{host}:{port}")
    print(f"Pasta de XLSX: {os.getenv('ISEQ_EXPORT_DIR', '')}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()

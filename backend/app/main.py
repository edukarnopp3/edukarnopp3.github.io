from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .jobs import JobStore


DEFAULT_EQUIPMENT = "1C:69:20:C7:31:D8"

app = FastAPI(title="ISEQ Export Backend", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = JobStore(storage_dir=os.getenv("ISEQ_STORAGE_DIR", "backend/storage"))


@app.middleware("http")
async def add_private_network_header(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


class JobRequest(BaseModel):
    equipment_id: str = Field(default=DEFAULT_EQUIPMENT)
    start: datetime
    end: datetime
    workers: int | None = Field(default=None, ge=1, le=6)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/iseq/jobs")
def create_job(payload: JobRequest) -> dict[str, object]:
    if payload.end <= payload.start:
        raise HTTPException(status_code=400, detail="A data final deve ser posterior à data inicial.")
    job = store.create_job(payload.equipment_id, payload.start, payload.end, workers=payload.workers)
    return asdict(job)


@app.get("/api/iseq/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, object]:
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return asdict(job)


@app.get("/api/iseq/jobs/{job_id}/data")
def get_job_data(job_id: str) -> dict[str, object]:
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    if job.status != "completed":
        raise HTTPException(status_code=409, detail="Job ainda não concluído.")
    return {"rows": store.get_data(job_id)}

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
import json
import os
import threading
import time
import uuid

from .collector import CollectorNotConfigured, ExportTask, build_collector, build_export_tasks
from .iseq_parser import parse_iseq_xlsx, records_to_wide_rows


MAX_RETRY_SECONDS = 15 * 60


@dataclass
class TaskState:
    equipment_id: str
    parameter: str
    start: str
    end: str
    status: str = "pending"
    attempts: int = 0
    last_error: str | None = None
    next_retry_at: str | None = None
    file_path: str | None = None


@dataclass
class JobState:
    id: str
    equipment_id: str
    start: str
    end: str
    status: str = "queued"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_attempts: int = 0
    message: str = "Aguardando início."
    tasks: list[TaskState] = field(default_factory=list)
    data_file: str | None = None


class JobStore:
    def __init__(self, storage_dir: str | os.PathLike[str] = "backend/storage"):
        self.storage_dir = Path(storage_dir)
        self.jobs_dir = self.storage_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.jobs: dict[str, JobState] = {}
        self.lock = threading.RLock()
        self.collector = build_collector()

    def create_job(self, equipment_id: str, start: datetime, end: datetime) -> JobState:
        job_id = uuid.uuid4().hex[:12]
        export_tasks = build_export_tasks(equipment_id, start, end)
        job = JobState(
            id=job_id,
            equipment_id=equipment_id,
            start=start.isoformat(timespec="seconds"),
            end=end.isoformat(timespec="seconds"),
            total_tasks=len(export_tasks),
            tasks=[self._task_to_state(task) for task in export_tasks],
        )
        with self.lock:
            self.jobs[job_id] = job
            self._save_job(job)

        worker = threading.Thread(target=self._run_job, args=(job_id,), daemon=True)
        worker.start()
        return job

    def get_job(self, job_id: str) -> JobState | None:
        with self.lock:
            job = self.jobs.get(job_id)
            if job:
                return job
        return self._load_job(job_id)

    def get_data(self, job_id: str) -> list[dict[str, object]]:
        job = self.get_job(job_id)
        if not job or not job.data_file:
            return []
        data_path = Path(job.data_file)
        if not data_path.exists():
            return []
        return json.loads(data_path.read_text(encoding="utf-8"))

    def _run_job(self, job_id: str) -> None:
        while True:
            with self.lock:
                job = self.jobs[job_id]
                pending = next((task for task in job.tasks if task.status != "completed"), None)
                if pending is None:
                    self._finalize_job(job)
                    return
                job.status = "running"
                job.message = f"Exportando {pending.parameter} ({pending.start} a {pending.end})."
                pending.status = "running"
                pending.attempts += 1
                job.updated_at = datetime.now().isoformat(timespec="seconds")
                self._save_job(job)

            try:
                file_path = self.collector.fetch_export(self._state_to_task(pending), self._job_export_dir(job_id))
                with self.lock:
                    job = self.jobs[job_id]
                    pending.status = "completed"
                    pending.file_path = str(file_path)
                    pending.last_error = None
                    pending.next_retry_at = None
                    job.completed_tasks = sum(1 for task in job.tasks if task.status == "completed")
                    job.message = f"{pending.parameter} concluído ({job.completed_tasks}/{job.total_tasks})."
                    job.updated_at = datetime.now().isoformat(timespec="seconds")
                    self._save_job(job)
            except CollectorNotConfigured as exc:
                self._record_retry(job_id, pending, exc, configuration_block=True)
            except Exception as exc:
                self._record_retry(job_id, pending, exc)

    def _record_retry(self, job_id: str, task: TaskState, exc: Exception, configuration_block: bool = False) -> None:
        delay = MAX_RETRY_SECONDS if configuration_block else min(2 ** min(task.attempts, 8), MAX_RETRY_SECONDS)
        next_retry = datetime.fromtimestamp(time.time() + delay)
        with self.lock:
            job = self.jobs[job_id]
            task.status = "waiting_configuration" if configuration_block else "retrying"
            task.last_error = str(exc)
            task.next_retry_at = next_retry.isoformat(timespec="seconds")
            job.status = task.status
            job.failed_attempts += 1
            job.message = f"{task.parameter} falhou: {task.last_error}. Nova tentativa em {delay}s."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)
        time.sleep(delay)

    def _finalize_job(self, job: JobState) -> None:
        files = [task.file_path for task in job.tasks if task.file_path]
        start = datetime.fromisoformat(job.start)
        end = datetime.fromisoformat(job.end)
        records = []
        for file_path in files:
            records.extend(
                record for record in parse_iseq_xlsx(file_path)
                if start <= record.data_local <= end
            )
        data = records_to_wide_rows(records)
        data_path = self._job_dir(job.id) / "data.json"
        data_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        with self.lock:
            job.status = "completed"
            job.data_file = str(data_path)
            job.message = f"Concluído com {len(data)} timestamps combinados."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)

    def _task_to_state(self, task: ExportTask) -> TaskState:
        return TaskState(
            equipment_id=task.equipment_id,
            parameter=task.parameter,
            start=task.start.isoformat(timespec="seconds"),
            end=task.end.isoformat(timespec="seconds"),
        )

    def _state_to_task(self, state: TaskState) -> ExportTask:
        return ExportTask(
            equipment_id=state.equipment_id,
            parameter=state.parameter,
            start=datetime.fromisoformat(state.start),
            end=datetime.fromisoformat(state.end),
        )

    def _job_dir(self, job_id: str) -> Path:
        path = self.jobs_dir / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _job_export_dir(self, job_id: str) -> Path:
        path = self._job_dir(job_id) / "exports"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _job_file(self, job_id: str) -> Path:
        return self._job_dir(job_id) / "job.json"

    def _save_job(self, job: JobState) -> None:
        self._job_file(job.id).write_text(json.dumps(asdict(job), ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_job(self, job_id: str) -> JobState | None:
        path = self._job_file(job_id)
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["tasks"] = [TaskState(**task) for task in raw.get("tasks", [])]
        job = JobState(**raw)
        with self.lock:
            self.jobs[job_id] = job
        return job

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
IDLE_SLEEP_SECONDS = 5
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "stale"}
ISEQ_UNAVAILABLE_MARKERS = (
    "HTTP 502",
    "HTTP 503",
    "HTTP 504",
    "Gateway Time-out",
    "Gateway Timeout",
)
ISEQ_AUTH_MARKERS = ("HTTP 401", "HTTP 403")


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
    worker_count: int = 1
    completed_tasks: int = 0
    attempted_tasks: int = 0
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
        try:
            configured_workers = int(os.getenv("ISEQ_JOB_WORKERS", "3"))
        except ValueError:
            configured_workers = 3
        self.default_workers = self._normalize_worker_count(configured_workers)

    def create_job(self, equipment_id: str, start: datetime, end: datetime, workers: object | None = None) -> JobState:
        job_id = uuid.uuid4().hex[:12]
        export_tasks = build_export_tasks(equipment_id, start, end)
        job = JobState(
            id=job_id,
            equipment_id=equipment_id,
            start=start.isoformat(timespec="seconds"),
            end=end.isoformat(timespec="seconds"),
            total_tasks=len(export_tasks),
            worker_count=self._normalize_worker_count(workers),
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
        with self.lock:
            job = self.jobs[job_id]
            worker_count = min(job.worker_count, max(1, job.total_tasks))
            job.status = "running"
            job.message = f"Exportando parâmetros com {worker_count} tarefas em paralelo."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)

        try:
            self.collector.preflight()
        except Exception as exc:
            stop_message = self._collector_stop_message(str(exc))
            if stop_message:
                with self.lock:
                    job = self.jobs[job_id]
                    job.status = "failed"
                    job.message = stop_message
                    job.updated_at = datetime.now().isoformat(timespec="seconds")
                    self._save_job(job)
                return

        workers = [
            threading.Thread(target=self._run_job_worker, args=(job_id,), daemon=True)
            for _ in range(worker_count)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        with self.lock:
            job = self.jobs[job_id]
            if not all(task.status == "completed" for task in job.tasks):
                return
            job.message = "Combinando arquivos exportados."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)
        self._finalize_job(job)

    def _run_job_worker(self, job_id: str) -> None:
        while True:
            with self.lock:
                job = self.jobs[job_id]
                if job.status in TERMINAL_STATUSES:
                    return
                stop_message = self._job_stop_message(job)
                if stop_message:
                    job.status = "failed"
                    job.message = stop_message
                    job.updated_at = datetime.now().isoformat(timespec="seconds")
                    self._save_job(job)
                    return
                pending = self._next_runnable_task(job)
                if pending is None:
                    if all(task.status == "completed" for task in job.tasks):
                        return
                    job.status = "running"
                    job.message = self._waiting_message(job)
                    job.updated_at = datetime.now().isoformat(timespec="seconds")
                    self._save_job(job)
                    sleep_seconds = self._seconds_until_next_retry(job)
                else:
                    sleep_seconds = 0
            if pending is None:
                time.sleep(sleep_seconds)
                continue
            self._run_task_once(job_id, pending)

    def _run_task_once(self, job_id: str, task: TaskState) -> None:
        with self.lock:
            job = self.jobs[job_id]
            job.status = "running"
            task.status = "running"
            task.attempts += 1
            active = sum(1 for item in job.tasks if item.status == "running")
            job.message = (
                f"Exportando {task.parameter} ({task.start} a {task.end}) "
                f"com {active} tarefas ativas."
            )
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)

        try:
            file_path = self.collector.fetch_export(self._state_to_task(task), self._job_export_dir(job_id))
            with self.lock:
                job = self.jobs[job_id]
                task.status = "completed"
                task.file_path = str(file_path)
                task.last_error = None
                task.next_retry_at = None
                self._refresh_counts(job)
                active = sum(1 for item in job.tasks if item.status == "running")
                job.message = f"{task.parameter} concluído ({job.completed_tasks}/{job.total_tasks}); {active} tarefas ativas."
                job.updated_at = datetime.now().isoformat(timespec="seconds")
                self._save_job(job)
        except CollectorNotConfigured as exc:
            self._record_retry(job_id, task, exc, configuration_block=True)
        except Exception as exc:
            self._record_retry(job_id, task, exc)

    def _record_retry(self, job_id: str, task: TaskState, exc: Exception, configuration_block: bool = False) -> None:
        delay = MAX_RETRY_SECONDS if configuration_block else min(2 ** min(task.attempts, 8), MAX_RETRY_SECONDS)
        next_retry = datetime.fromtimestamp(time.time() + delay)
        with self.lock:
            job = self.jobs[job_id]
            task.status = "waiting_configuration" if configuration_block else "retrying"
            task.last_error = str(exc)
            task.next_retry_at = next_retry.isoformat(timespec="seconds")
            job.status = "running"
            job.failed_attempts += 1
            self._refresh_counts(job)
            job.message = f"{task.parameter} falhou: {task.last_error}. Vou seguir com outras tarefas e tentar de novo em {delay}s."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            self._save_job(job)

    def _next_runnable_task(self, job: JobState) -> TaskState | None:
        now = datetime.now()
        for status in ("pending", "retrying"):
            for task in job.tasks:
                if task.status != status:
                    continue
                if status == "retrying" and task.next_retry_at:
                    try:
                        if datetime.fromisoformat(task.next_retry_at) > now:
                            continue
                    except ValueError:
                        pass
                return task
        return None

    def _seconds_until_next_retry(self, job: JobState) -> int:
        retry_times = []
        for task in job.tasks:
            if task.status != "retrying" or not task.next_retry_at:
                continue
            try:
                retry_times.append(datetime.fromisoformat(task.next_retry_at))
            except ValueError:
                pass
        if not retry_times:
            return IDLE_SLEEP_SECONDS
        seconds = (min(retry_times) - datetime.now()).total_seconds()
        return max(IDLE_SLEEP_SECONDS, min(int(seconds), MAX_RETRY_SECONDS))

    def _waiting_message(self, job: JobState) -> str:
        pending = sum(1 for task in job.tasks if task.status == "pending")
        retrying = sum(1 for task in job.tasks if task.status == "retrying")
        return f"Aguardando novas tentativas. Pendentes: {pending}; em retentativa: {retrying}."

    def _refresh_counts(self, job: JobState) -> None:
        job.completed_tasks = sum(1 for task in job.tasks if task.status == "completed")
        job.attempted_tasks = sum(1 for task in job.tasks if task.attempts > 0 or task.status == "completed")

    def _job_stop_message(self, job: JobState) -> str | None:
        if not job.tasks or job.completed_tasks > 0:
            return None
        if not all(task.attempts > 0 for task in job.tasks):
            return None

        errors = [task.last_error or "" for task in job.tasks]
        if errors and all(self._has_marker(error, ISEQ_UNAVAILABLE_MARKERS) for error in errors):
            return self._iseq_unavailable_message()
        if errors and all(self._has_marker(error, ISEQ_AUTH_MARKERS) for error in errors):
            return self._iseq_auth_message()
        return None

    def _collector_stop_message(self, error: str) -> str | None:
        if self._has_marker(error, ISEQ_UNAVAILABLE_MARKERS):
            return self._iseq_unavailable_message()
        if self._has_marker(error, ISEQ_AUTH_MARKERS):
            return self._iseq_auth_message()
        return None

    def _iseq_unavailable_message(self) -> str:
        return (
            "ISEQ indisponível: a API retornou erro 502/503/504 antes de gerar os relatórios. "
            "O site da ISEQ não está entregando dados agora. Tente novamente mais tarde ou carregue os XLSX manualmente."
        )

    def _iseq_auth_message(self) -> str:
        return (
            "Token ISEQ recusado: a API retornou 401/403. "
            "Abra o login automático novamente para capturar um token novo."
        )

    def _has_marker(self, text: str, markers: tuple[str, ...]) -> bool:
        lower = text.lower()
        return any(marker.lower() in lower for marker in markers)

    def _normalize_worker_count(self, value: object | None) -> int:
        if value is None or value == "":
            return self.default_workers if hasattr(self, "default_workers") else 3
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = self.default_workers if hasattr(self, "default_workers") else 3
        return max(1, min(parsed, 6))

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
        if job.status in {"queued", "running", "retrying", "waiting_configuration"}:
            job.status = "failed"
            job.message = "Backend foi reiniciado antes de concluir. Inicie uma nova busca."
            job.updated_at = datetime.now().isoformat(timespec="seconds")
        with self.lock:
            self.jobs[job_id] = job
            if job.status == "failed":
                self._save_job(job)
        return job

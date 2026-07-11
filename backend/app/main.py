from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import os
import threading
import time

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .collector import ApiReportCollector, IseqAuthenticationError, authenticate_iseq
from .database import DatabaseStore, SessionContext
from .jobs import JobStore


DEFAULT_EQUIPMENT = "1C:69:20:C7:31:D8"
DEFAULT_CORS_ORIGINS = (
    "https://edukarnopp3.github.io,"
    "http://127.0.0.1:8765,http://localhost:8765,"
    "http://127.0.0.1:8000,http://localhost:8000"
)
STORAGE_DIR = os.getenv("ISEQ_STORAGE_DIR", "storage")


def configured_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", DEFAULT_CORS_ORIGINS)
    if raw.strip() == "*" and os.getenv("RENDER"):
        raw = DEFAULT_CORS_ORIGINS
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


database = DatabaseStore(
    database_url=os.getenv("DATABASE_URL"),
    app_secret=os.getenv("APP_SECRET"),
    storage_dir=STORAGE_DIR,
)


def collector_for_user(user_id: str) -> ApiReportCollector:
    return ApiReportCollector(
        token=database.get_iseq_token(user_id),
        api_base=os.getenv("ISEQ_API_BASE"),
    )


store = JobStore(
    storage_dir=STORAGE_DIR,
    collector_factory=collector_for_user,
    repository=database,
)

app = FastAPI(title="ISEQ Export Backend", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=configured_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


class LoginRequest(BaseModel):
    username_or_email: str = Field(min_length=1, max_length=320)
    password: str = Field(min_length=1, max_length=256)


class JobRequest(BaseModel):
    equipment_id: str = Field(default=DEFAULT_EQUIPMENT, min_length=1, max_length=64)
    start: datetime
    end: datetime
    workers: int | None = Field(default=None, ge=1, le=6)


class LoginLimiter:
    def __init__(self, max_attempts: int = 8, window_seconds: int = 600):
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.failures: dict[str, list[float]] = {}
        self.lock = threading.Lock()

    def ensure_allowed(self, key: str) -> None:
        now = time.time()
        with self.lock:
            recent = [stamp for stamp in self.failures.get(key, []) if now - stamp < self.window_seconds]
            self.failures[key] = recent
            if len(recent) >= self.max_attempts:
                raise api_error(
                    429,
                    "login_rate_limited",
                    "Muitas tentativas de login. Aguarde alguns minutos e tente novamente.",
                )

    def record_failure(self, key: str) -> None:
        with self.lock:
            self.failures.setdefault(key, []).append(time.time())

    def clear(self, key: str) -> None:
        with self.lock:
            self.failures.pop(key, None)


login_limiter = LoginLimiter()


def api_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={"code": code, "message": message},
    )


def bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise api_error(401, "session_invalid", "Entre com sua conta ISEQ para continuar.")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise api_error(401, "session_invalid", "Entre com sua conta ISEQ para continuar.")
    return token


def require_session(authorization: str | None = Header(default=None)) -> SessionContext:
    session = database.get_session(bearer_token(authorization))
    if not session:
        raise api_error(401, "session_invalid", "Sua sessão terminou. Entre novamente.")
    return session


def require_hosted_security_configuration() -> None:
    if not os.getenv("RENDER"):
        return
    if not database.is_persistent:
        raise api_error(
            503,
            "database_not_configured",
            "O banco persistente ainda não foi configurado no backend.",
        )
    if not database.secret_is_configured:
        raise api_error(
            503,
            "app_secret_not_configured",
            "A chave de segurança do backend ainda não foi configurada.",
        )


def is_iseq_auth_error(message: str) -> bool:
    lowered = message.lower()
    return "http 401" in lowered or "http 403" in lowered or "token" in lowered


@app.middleware("http")
async def add_private_network_header(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/api/health")
def health() -> dict[str, object]:
    hosted = bool(os.getenv("RENDER"))
    return {
        "status": "ok",
        "database": database.backend_name,
        "persistent_storage": database.is_persistent,
        "auth_ready": (not hosted) or (database.is_persistent and database.secret_is_configured),
    }


@app.post("/api/auth/iseq/login")
def login(payload: LoginRequest, request: Request) -> dict[str, object]:
    require_hosted_security_configuration()
    client_key = request.client.host if request.client else "unknown"
    login_limiter.ensure_allowed(client_key)

    try:
        token, profile = authenticate_iseq(payload.username_or_email, payload.password)
    except IseqAuthenticationError as exc:
        login_limiter.record_failure(client_key)
        raise api_error(401, "invalid_iseq_credentials", str(exc)) from exc
    except Exception as exc:
        raise api_error(502, "iseq_login_unavailable", str(exc)) from exc

    try:
        user = database.connect_iseq_account(
            login_hint=payload.username_or_email,
            token=token,
            profile=profile,
        )
        session_hours = int(os.getenv("APP_SESSION_HOURS", "12"))
        session_token = database.create_session(str(user["id"]), hours=session_hours)
    except Exception as exc:
        raise api_error(
            503,
            "database_unavailable",
            "Não foi possível salvar a conexão com a ISEQ no banco de dados.",
        ) from exc

    login_limiter.clear(client_key)
    equipment: list[dict[str, str]] = []
    try:
        equipment = ApiReportCollector(token=token, api_base=os.getenv("ISEQ_API_BASE")).list_equipment()
        database.save_sensors(str(user["id"]), equipment)
    except Exception:
        pass

    return {
        "session_token": session_token,
        "expires_in_hours": max(1, min(session_hours, 168)),
        "user": user,
        "equipment": equipment,
    }


@app.get("/api/auth/session")
def session_status(session: SessionContext = Depends(require_session)) -> dict[str, object]:
    return {"user": session.public_user()}


@app.post("/api/auth/logout")
def logout(
    authorization: str | None = Header(default=None),
    _session: SessionContext = Depends(require_session),
) -> dict[str, bool]:
    database.revoke_session(bearer_token(authorization))
    return {"ok": True}


@app.get("/api/iseq/equipment")
def list_equipment(session: SessionContext = Depends(require_session)) -> dict[str, object]:
    try:
        equipment = store.list_equipment(session.user_id)
        database.save_sensors(session.user_id, equipment)
        return {"equipment": equipment}
    except Exception as exc:
        message = str(exc)
        if is_iseq_auth_error(message):
            database.mark_connection_expired(session.user_id)
            raise api_error(
                401,
                "iseq_reauthentication_required",
                "Sua conexão ISEQ expirou. Entre novamente para renová-la.",
            ) from exc
        raise api_error(502, "iseq_unavailable", f"Falha ao consultar a ISEQ: {message}") from exc


@app.post("/api/iseq/jobs")
def create_job(
    payload: JobRequest,
    session: SessionContext = Depends(require_session),
) -> dict[str, object]:
    if payload.end <= payload.start:
        raise api_error(400, "invalid_period", "A data final deve ser posterior à data inicial.")
    if session.connection_status != "connected":
        raise api_error(
            401,
            "iseq_reauthentication_required",
            "Sua conexão ISEQ expirou. Entre novamente para renová-la.",
        )
    equipment_id = payload.equipment_id.strip().upper()
    if not database.user_has_sensor(session.user_id, equipment_id):
        try:
            equipment = store.list_equipment(session.user_id)
            database.save_sensors(session.user_id, equipment)
        except Exception as exc:
            if is_iseq_auth_error(str(exc)):
                database.mark_connection_expired(session.user_id)
                raise api_error(
                    401,
                    "iseq_reauthentication_required",
                    "Sua conexão ISEQ expirou. Entre novamente para renová-la.",
                ) from exc
            raise api_error(502, "iseq_unavailable", "Não foi possível validar o ambiente na ISEQ.") from exc
    if not database.user_has_sensor(session.user_id, equipment_id):
        raise api_error(403, "sensor_not_allowed", "Este ambiente não pertence à conta conectada.")
    job = store.create_job(
        equipment_id,
        payload.start,
        payload.end,
        workers=payload.workers,
        user_id=session.user_id,
    )
    return asdict(job)


@app.get("/api/iseq/jobs/{job_id}")
def get_job(
    job_id: str,
    session: SessionContext = Depends(require_session),
) -> dict[str, object]:
    job = store.get_job(job_id)
    if not job or job.user_id != session.user_id:
        raise api_error(404, "job_not_found", "Importação não encontrada.")
    if job.status == "failed" and is_iseq_auth_error(job.message):
        database.mark_connection_expired(session.user_id)
        raise api_error(
            401,
            "iseq_reauthentication_required",
            "Sua conexão ISEQ expirou durante a importação. Entre novamente.",
        )
    return asdict(job)


@app.get("/api/iseq/jobs/{job_id}/data")
def get_job_data(
    job_id: str,
    session: SessionContext = Depends(require_session),
) -> dict[str, object]:
    job = store.get_job(job_id)
    if not job or job.user_id != session.user_id:
        raise api_error(404, "job_not_found", "Importação não encontrada.")
    if job.status != "completed":
        raise api_error(409, "job_not_completed", "A importação ainda não foi concluída.")
    return {"rows": store.get_data(job_id)}

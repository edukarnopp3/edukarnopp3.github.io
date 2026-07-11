from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Iterable
import uuid

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


DEFAULT_DEV_SECRET = "development-only-change-before-hosting"


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalize_database_url(value: str | None, storage_dir: str | os.PathLike[str]) -> str:
    if not value:
        db_path = (Path(storage_dir) / "airquality.db").resolve().as_posix()
        return f"sqlite:///{db_path}"
    if value.startswith("postgres://"):
        return "postgresql+psycopg://" + value[len("postgres://") :]
    if value.startswith("postgresql://"):
        return "postgresql+psycopg://" + value[len("postgresql://") :]
    return value


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    account_key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    iseq_user_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    display_name: Mapped[str] = mapped_column(String(255), default="Usuário ISEQ")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class IseqConnection(Base):
    __tablename__ = "iseq_connections"

    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    login_hint: Mapped[str | None] = mapped_column(String(320), nullable=True)
    encrypted_token: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="connected")
    connected_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_validated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class AppSession(Base):
    __tablename__ = "app_sessions"

    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)


class Sensor(Base):
    __tablename__ = "sensors"
    __table_args__ = (UniqueConstraint("user_id", "mac", name="uq_sensor_user_mac"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    mac: Mapped[str] = mapped_column(String(64))
    location: Mapped[str] = mapped_column(String(255))
    label: Mapped[str] = mapped_column(String(384))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class ImportJob(Base):
    __tablename__ = "import_jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    equipment_id: Mapped[str] = mapped_column(String(64), index=True)
    period_start: Mapped[datetime] = mapped_column(DateTime)
    period_end: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(32), index=True)
    state_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Reading(Base):
    __tablename__ = "readings"

    sensor_id: Mapped[int] = mapped_column(ForeignKey("sensors.id"), primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    source_job_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    co2: Mapped[float | None] = mapped_column(Float, nullable=True)
    nox: Mapped[float | None] = mapped_column(Float, nullable=True)
    voc: Mapped[float | None] = mapped_column(Float, nullable=True)
    pm10: Mapped[float | None] = mapped_column(Float, nullable=True)
    pm25: Mapped[float | None] = mapped_column(Float, nullable=True)
    pm1: Mapped[float | None] = mapped_column(Float, nullable=True)
    humidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    temperature: Mapped[float | None] = mapped_column(Float, nullable=True)
    pressure: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


@dataclass(frozen=True)
class SessionContext:
    user_id: str
    display_name: str
    username: str | None
    email: str | None
    connection_status: str

    def public_user(self) -> dict[str, str | None]:
        return {
            "id": self.user_id,
            "display_name": self.display_name,
            "username": self.username,
            "email": self.email,
            "connection_status": self.connection_status,
        }


class TokenCipher:
    def __init__(self, secret: str):
        digest = hashlib.sha256(secret.encode("utf-8")).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(digest))

    def encrypt(self, token: str) -> str:
        return self._fernet.encrypt(token.encode("utf-8")).decode("ascii")

    def decrypt(self, encrypted_token: str) -> str:
        try:
            return self._fernet.decrypt(encrypted_token.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise RuntimeError("Não foi possível descriptografar o token ISEQ. Verifique APP_SECRET.") from exc


class DatabaseStore:
    def __init__(
        self,
        database_url: str | None = None,
        app_secret: str | None = None,
        storage_dir: str | os.PathLike[str] = "storage",
    ):
        self.database_url = normalize_database_url(database_url, storage_dir)
        self.is_persistent = not self.database_url.startswith("sqlite:")
        self.secret_is_configured = bool(app_secret and len(app_secret) >= 32)
        self.cipher = TokenCipher(app_secret or DEFAULT_DEV_SECRET)

        connect_args: dict[str, object] = {}
        if self.database_url.startswith("sqlite:"):
            Path(storage_dir).mkdir(parents=True, exist_ok=True)
            connect_args["check_same_thread"] = False
        elif self.database_url.startswith("postgresql"):
            connect_args["prepare_threshold"] = None

        self.engine = create_engine(
            self.database_url,
            connect_args=connect_args,
            pool_pre_ping=True,
        )
        Base.metadata.create_all(self.engine)
        if self.engine.dialect.name == "postgresql":
            self._enable_row_level_security()

    @property
    def backend_name(self) -> str:
        return self.engine.dialect.name

    def connect_iseq_account(
        self,
        login_hint: str,
        token: str,
        profile: dict[str, object],
    ) -> dict[str, str | None]:
        identity = self._profile_value(profile, "id", "user_id", "userId", "email", "username") or login_hint
        account_key = hashlib.sha256(str(identity).strip().lower().encode("utf-8")).hexdigest()
        username = self._profile_value(profile, "username", "usuario", "name")
        email = self._profile_value(profile, "email")
        display_name = self._profile_value(profile, "nome", "display_name", "displayName", "name", "username")
        display_name = display_name or username or email or login_hint

        with Session(self.engine) as session:
            user = session.scalar(select(User).where(User.account_key == account_key))
            if not user:
                user = User(id=str(uuid.uuid4()), account_key=account_key, display_name=str(display_name))
                session.add(user)
            user.iseq_user_id = str(identity)
            user.username = str(username) if username else None
            user.email = str(email) if email else None
            user.display_name = str(display_name)
            user.updated_at = utcnow()
            session.flush()

            connection = session.get(IseqConnection, user.id)
            if not connection:
                connection = IseqConnection(
                    user_id=user.id,
                    login_hint=login_hint,
                    encrypted_token=self.cipher.encrypt(token),
                )
                session.add(connection)
            else:
                connection.login_hint = login_hint
                connection.encrypted_token = self.cipher.encrypt(token)
                connection.status = "connected"
                connection.last_validated_at = utcnow()
            session.commit()
            return self._public_user(user, connection.status)

    def create_session(self, user_id: str, hours: int = 12) -> str:
        raw_token = secrets.token_urlsafe(48)
        token_hash = self._hash_session_token(raw_token)
        now = utcnow()
        with Session(self.engine) as session:
            session.add(
                AppSession(
                    token_hash=token_hash,
                    user_id=user_id,
                    created_at=now,
                    last_seen_at=now,
                    expires_at=now + timedelta(hours=max(1, min(hours, 168))),
                )
            )
            session.commit()
        return raw_token

    def get_session(self, raw_token: str) -> SessionContext | None:
        if not raw_token:
            return None
        token_hash = self._hash_session_token(raw_token)
        now = utcnow()
        with Session(self.engine) as session:
            app_session = session.get(AppSession, token_hash)
            if not app_session or app_session.revoked or app_session.expires_at <= now:
                return None
            user = session.get(User, app_session.user_id)
            connection = session.get(IseqConnection, app_session.user_id)
            if not user or not connection:
                return None
            if (now - app_session.last_seen_at).total_seconds() >= 300:
                app_session.last_seen_at = now
                session.commit()
            return SessionContext(
                user_id=user.id,
                display_name=user.display_name,
                username=user.username,
                email=user.email,
                connection_status=connection.status,
            )

    def revoke_session(self, raw_token: str) -> None:
        with Session(self.engine) as session:
            app_session = session.get(AppSession, self._hash_session_token(raw_token))
            if app_session:
                app_session.revoked = True
                session.commit()

    def get_iseq_token(self, user_id: str) -> str:
        with Session(self.engine) as session:
            connection = session.get(IseqConnection, user_id)
            if not connection or connection.status != "connected":
                raise RuntimeError("A conta ISEQ precisa ser conectada novamente.")
            return self.cipher.decrypt(connection.encrypted_token)

    def mark_connection_expired(self, user_id: str) -> None:
        with Session(self.engine) as session:
            connection = session.get(IseqConnection, user_id)
            if connection:
                connection.status = "reauthentication_required"
                session.commit()

    def save_sensors(self, user_id: str, equipment: Iterable[dict[str, str]]) -> None:
        with Session(self.engine) as session:
            for item in equipment:
                mac = str(item.get("mac") or "").strip().upper()
                if not mac:
                    continue
                sensor = session.scalar(
                    select(Sensor).where(Sensor.user_id == user_id, Sensor.mac == mac)
                )
                location = str(item.get("location") or item.get("label") or mac).strip()
                label = str(item.get("label") or location or mac).strip()
                if not sensor:
                    session.add(Sensor(user_id=user_id, mac=mac, location=location, label=label))
                else:
                    sensor.location = location
                    sensor.label = label
                    sensor.updated_at = utcnow()
            session.commit()

    def user_has_sensor(self, user_id: str, equipment_id: str) -> bool:
        with Session(self.engine) as session:
            sensor = session.scalar(
                select(Sensor.id).where(
                    Sensor.user_id == user_id,
                    Sensor.mac == equipment_id.strip().upper(),
                )
            )
            return sensor is not None

    def save_import_job(self, state: dict[str, object]) -> None:
        with Session(self.engine) as session:
            job_id = str(state["id"])
            job = session.get(ImportJob, job_id)
            values = {
                "user_id": str(state.get("user_id") or "legacy"),
                "equipment_id": str(state["equipment_id"]),
                "period_start": datetime.fromisoformat(str(state["start"])),
                "period_end": datetime.fromisoformat(str(state["end"])),
                "status": str(state.get("status") or "queued"),
                "state_json": json.dumps(state, ensure_ascii=False),
                "updated_at": utcnow(),
            }
            if not job:
                session.add(ImportJob(id=job_id, created_at=utcnow(), **values))
            else:
                for key, value in values.items():
                    setattr(job, key, value)
            session.commit()

    def load_import_job(self, job_id: str) -> dict[str, object] | None:
        with Session(self.engine) as session:
            job = session.get(ImportJob, job_id)
            return json.loads(job.state_json) if job else None

    def save_readings(
        self,
        user_id: str,
        equipment_id: str,
        job_id: str,
        rows: list[dict[str, object]],
    ) -> None:
        sensor_id = self._ensure_sensor(user_id, equipment_id)
        payload = []
        saved_at = utcnow()
        for row in rows:
            recorded_at = row.get("data_local")
            if not recorded_at:
                continue
            if not isinstance(recorded_at, datetime):
                recorded_at = datetime.fromisoformat(str(recorded_at))
            payload.append(
                {
                    "sensor_id": sensor_id,
                    "recorded_at": recorded_at,
                    "source_job_id": job_id,
                    "co2": self._optional_float(row.get("CO2")),
                    "nox": self._optional_float(row.get("NOx")),
                    "voc": self._optional_float(row.get("VOC")),
                    "pm10": self._optional_float(row.get("PM10")),
                    "pm25": self._optional_float(row.get("PM2.5")),
                    "pm1": self._optional_float(row.get("PM1")),
                    "humidity": self._optional_float(row.get("Umid")),
                    "temperature": self._optional_float(row.get("Temp")),
                    "pressure": self._optional_float(row.get("Pressao")),
                    "updated_at": saved_at,
                }
            )

        metric_columns = (
            "co2",
            "nox",
            "voc",
            "pm10",
            "pm25",
            "pm1",
            "humidity",
            "temperature",
            "pressure",
        )
        for chunk_start in range(0, len(payload), 1000):
            chunk = payload[chunk_start : chunk_start + 1000]
            with Session(self.engine) as session:
                if self.engine.dialect.name == "postgresql":
                    statement = postgres_insert(Reading).values(chunk)
                else:
                    statement = sqlite_insert(Reading).values(chunk)
                update_values = {
                    "source_job_id": statement.excluded.source_job_id,
                    "updated_at": statement.excluded.updated_at,
                }
                update_values.update(
                    {
                        key: func.coalesce(getattr(statement.excluded, key), getattr(Reading, key))
                        for key in metric_columns
                    }
                )
                statement = statement.on_conflict_do_update(
                    index_elements=[Reading.sensor_id, Reading.recorded_at],
                    set_=update_values,
                )
                session.execute(statement)
                session.commit()

    def get_readings(
        self,
        user_id: str,
        equipment_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, object]]:
        with Session(self.engine) as session:
            sensor = session.scalar(
                select(Sensor).where(
                    Sensor.user_id == user_id,
                    Sensor.mac == equipment_id.strip().upper(),
                )
            )
            if not sensor:
                return []
            readings = session.scalars(
                select(Reading)
                .where(
                    Reading.sensor_id == sensor.id,
                    Reading.recorded_at >= start,
                    Reading.recorded_at <= end,
                )
                .order_by(Reading.recorded_at)
            ).all()
            return [self._reading_to_row(reading) for reading in readings]

    def _ensure_sensor(self, user_id: str, equipment_id: str) -> int:
        mac = equipment_id.strip().upper()
        with Session(self.engine) as session:
            sensor = session.scalar(
                select(Sensor).where(Sensor.user_id == user_id, Sensor.mac == mac)
            )
            if not sensor:
                sensor = Sensor(user_id=user_id, mac=mac, location=mac, label=mac)
                session.add(sensor)
                session.commit()
                session.refresh(sensor)
            return sensor.id

    def _reading_to_row(self, reading: Reading) -> dict[str, object]:
        row: dict[str, object] = {
            "data_local": reading.recorded_at.isoformat(timespec="seconds")
        }
        fields = (
            ("CO2", reading.co2),
            ("NOx", reading.nox),
            ("VOC", reading.voc),
            ("PM10", reading.pm10),
            ("PM2.5", reading.pm25),
            ("PM1", reading.pm1),
            ("Umid", reading.humidity),
            ("Temp", reading.temperature),
            ("Pressao", reading.pressure),
        )
        for name, value in fields:
            if value is not None:
                row[name] = value
        return row

    def _public_user(self, user: User, status: str) -> dict[str, str | None]:
        return {
            "id": user.id,
            "display_name": user.display_name,
            "username": user.username,
            "email": user.email,
            "connection_status": status,
        }

    def _profile_value(self, profile: dict[str, object], *keys: str) -> str | None:
        for key in keys:
            value = profile.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    def _hash_session_token(self, raw_token: str) -> str:
        return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

    def _optional_float(self, value: object) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _enable_row_level_security(self) -> None:
        table_names = (
            "users",
            "iseq_connections",
            "app_sessions",
            "sensors",
            "import_jobs",
            "readings",
        )
        with self.engine.begin() as connection:
            for table_name in table_names:
                connection.exec_driver_sql(
                    f'ALTER TABLE public."{table_name}" ENABLE ROW LEVEL SECURITY'
                )

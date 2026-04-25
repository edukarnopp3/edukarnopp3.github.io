from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import json
import os
from pathlib import Path
import shutil
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from .iseq_parser import KNOWN_PARAMETERS, normalize_parameter, parse_iseq_xlsx


@dataclass(frozen=True)
class ExportTask:
    equipment_id: str
    parameter: str
    start: datetime
    end: datetime


class CollectorNotConfigured(RuntimeError):
    pass


class IseqCollector:
    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        raise NotImplementedError


class ApiReportCollector(IseqCollector):
    def __init__(self, token: str, api_base: str | None = None):
        self.token = token.strip()
        self.api_base = (api_base or "https://sensores.iseq.com.br/api-v2-staging").rstrip("/") + "/"
        self.timeout_seconds = int(os.getenv("ISEQ_REPORT_TIMEOUT_SECONDS", "900"))
        self.poll_seconds = int(os.getenv("ISEQ_REPORT_POLL_SECONDS", "5"))
        self.request_timeout_seconds = int(os.getenv("ISEQ_REQUEST_TIMEOUT_SECONDS", "180"))
        self.download_timeout_seconds = int(os.getenv("ISEQ_DOWNLOAD_TIMEOUT_SECONDS", "300"))
        self.utc_offset_hours = int(os.getenv("ISEQ_LOCAL_UTC_OFFSET_HOURS", "3"))

    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        report = self._safe_find_ready_report(task) or self._generate_and_wait(task)
        report_id = report.get("id")
        if not report_id:
            raise RuntimeError(f"Relatorio pronto sem id para {task.parameter}.")

        data = report.get("_downloaded_bytes")
        if not isinstance(data, bytes):
            data = self._request_bytes(f"reports/{report_id}/download")
        destination_dir.mkdir(parents=True, exist_ok=True)
        filename = self._safe_filename(report.get("nome") or f"{task.parameter}_{task.start:%Y%m%d}_{task.end:%Y%m%d}.xlsx")
        if not filename.lower().endswith(".xlsx"):
            filename += ".xlsx"
        destination = destination_dir / filename
        destination.write_bytes(data)
        return destination

    def _safe_find_ready_report(self, task: ExportTask) -> dict[str, object] | None:
        try:
            return self._find_ready_report(task)
        except Exception:
            return None

    def _generate_and_wait(self, task: ExportTask) -> dict[str, object]:
        payload = {
            "mac": task.equipment_id,
            "parametro": self._api_parameter(task.parameter),
            "dataIni": self._api_datetime(task.start),
            "dataFim": self._api_datetime(task.end),
            "formato": "xlsx",
        }
        response = self._request_json("generate-report", method="POST", payload=payload)
        direct_id = self._extract_report_id(response)
        deadline = time.time() + self.timeout_seconds
        last_error: Exception | None = None

        while time.time() < deadline:
            if direct_id:
                direct_download = self._try_direct_download(direct_id, task)
                if direct_download:
                    return direct_download
                try:
                    report = self._find_report_by_id(direct_id)
                    if report and self._is_ready(report):
                        return report
                except Exception as exc:
                    last_error = exc
            try:
                report = self._find_ready_report(task)
                if report:
                    return report
            except Exception as exc:
                last_error = exc
            time.sleep(self.poll_seconds)

        detail = f" Ultimo erro: {last_error}" if last_error else ""
        raise TimeoutError(f"Relatorio {task.parameter} nao ficou pronto em {self.timeout_seconds}s.{detail}")

    def _try_direct_download(self, report_id: object, task: ExportTask) -> dict[str, object] | None:
        try:
            data = self._request_bytes(f"reports/{report_id}/download", timeout=min(45, self.download_timeout_seconds))
        except Exception:
            return None
        if not data.startswith(b"PK"):
            return None
        return {
            "id": report_id,
            "nome": f"{task.parameter}_{task.start:%Y%m%d}_{task.end:%Y%m%d}.xlsx",
            "_downloaded_bytes": data,
        }

    def _find_ready_report(self, task: ExportTask) -> dict[str, object] | None:
        reports = self._list_reports()
        matches = [report for report in reports if self._matches_task(report, task)]
        ready = [report for report in matches if self._is_ready(report)]
        if not ready:
            return None
        return max(ready, key=lambda report: str(report.get("id") or report.get("createdAt") or report.get("nome") or ""))

    def _find_report_by_id(self, report_id: object) -> dict[str, object] | None:
        for report in self._list_reports():
            if str(report.get("id")) == str(report_id):
                return report
        return None

    def _list_reports(self) -> list[dict[str, object]]:
        payload = self._request_json("reports")
        return payload if isinstance(payload, list) else []

    def _matches_task(self, report: dict[str, object], task: ExportTask) -> bool:
        parameter = normalize_parameter(report.get("parametro"))
        if parameter != task.parameter:
            return False

        report_mac = str(report.get("mac") or "")
        report_name = str(report.get("nome") or "")
        mac_variants = {task.equipment_id, task.equipment_id.replace(":", "_"), task.equipment_id.replace(":", "-")}
        if report_mac and not any(variant in report_mac for variant in mac_variants):
            return False
        if not report_mac and report_name and any(variant in report_name for variant in mac_variants):
            pass

        data_ini = self._parse_report_datetime(report.get("dataIni"))
        data_fim = self._parse_report_datetime(report.get("dataFim"))
        if data_ini and abs((data_ini - self._api_naive_utc(task.start)).total_seconds()) > 120:
            return False
        if data_fim and abs((data_fim - self._api_naive_utc(task.end)).total_seconds()) > 120:
            return False
        return True

    def _is_ready(self, report: dict[str, object]) -> bool:
        return str(report.get("status") or "").strip().lower() == "pronto"

    def _api_parameter(self, parameter: str) -> str:
        return {
            "PM2.5": "PM2.5",
            "Pressao": "pressao",
        }.get(parameter, parameter)

    def _api_datetime(self, value: datetime) -> str:
        return self._api_naive_utc(value).strftime("%Y-%m-%d %H:%M:%S")

    def _api_naive_utc(self, value: datetime) -> datetime:
        return value + timedelta(hours=self.utc_offset_hours)

    def _parse_report_datetime(self, value: object) -> datetime | None:
        if not value:
            return None
        text = str(value).replace("T", " ").replace("Z", "").split(".")[0]
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _extract_report_id(self, response: object) -> object | None:
        if not isinstance(response, dict):
            return None
        for key in ("id", "reportId", "report_id"):
            if response.get(key):
                return response[key]
        report = response.get("report")
        if isinstance(report, dict):
            return report.get("id")
        return None

    def _request_json(self, endpoint: str, method: str = "GET", payload: dict[str, object] | None = None) -> object:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            urljoin(self.api_base, endpoint),
            data=data,
            method=method,
            headers=self._headers(json_payload=payload is not None),
        )
        try:
            with urlopen(request, timeout=self.request_timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ISEQ API HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Falha de conexao com ISEQ API: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"Tempo esgotado na API ISEQ apos {self.request_timeout_seconds}s.") from exc
        return json.loads(raw) if raw else {}

    def _request_bytes(self, endpoint: str, timeout: int | None = None) -> bytes:
        request = Request(urljoin(self.api_base, endpoint), method="GET", headers=self._headers())
        timeout_seconds = timeout or self.download_timeout_seconds
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ISEQ download HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Falha de conexao no download ISEQ: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"Tempo esgotado no download ISEQ apos {timeout_seconds}s.") from exc

    def _headers(self, json_payload: bool = False) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {self.token}",
            "Origin": "https://sensores.iseq.com.br",
            "Referer": "https://sensores.iseq.com.br/relatorios",
            "User-Agent": "Mozilla/5.0",
        }
        if json_payload:
            headers["Content-Type"] = "application/json"
        return headers

    def _safe_filename(self, name: object) -> str:
        text = str(name or "relatorio.xlsx")
        return "".join(ch if ch.isalnum() or ch in "._- " else "_" for ch in text).strip() or "relatorio.xlsx"


class LocalExportCollector(IseqCollector):
    """Uses already downloaded ISEQ XLSX files.

    Set ISEQ_EXPORT_DIR to a directory containing files like
    CO2_1C_69_20_C7_31_D8_2026-04-24T20-35.xlsx. This is useful while the
    authenticated website automation is not wired yet.
    """

    def __init__(self, export_dir: str | os.PathLike[str]):
        self.export_dir = Path(export_dir)

    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        parameter_matches = []
        period_matches = []
        equipment_token = task.equipment_id.replace(":", "_").lower()
        for path in self.export_dir.glob("*.xlsx"):
            parameter = normalize_parameter(path.stem.split("_", 1)[0])
            if parameter != task.parameter:
                continue
            normalized_name = path.name.lower().replace(".", "")
            if equipment_token not in normalized_name:
                continue
            parameter_matches.append(path)
            if self._has_records_for_period(path, task):
                period_matches.append(path)

        if not parameter_matches:
            raise FileNotFoundError(f"Nenhum XLSX local encontrado para {task.parameter} em {self.export_dir}.")
        if not period_matches:
            raise FileNotFoundError(
                f"Há XLSX local para {task.parameter}, mas nenhum cobre "
                f"{task.start:%d/%m/%Y %H:%M} a {task.end:%d/%m/%Y %H:%M}."
            )

        source = max(period_matches, key=lambda p: p.stat().st_mtime)
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / f"{task.parameter.replace('.', '_')}_{int(time.time())}.xlsx"
        shutil.copy2(source, destination)
        return destination

    def _has_records_for_period(self, path: Path, task: ExportTask) -> bool:
        try:
            for record in parse_iseq_xlsx(path):
                if record.parameter == task.parameter and task.start <= record.data_local < task.end:
                    return True
        except Exception:
            return False
        return False


class NotConfiguredCollector(IseqCollector):
    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        raise CollectorNotConfigured(
            "Coletor ISEQ não configurado. Configure ISEQ_EXPORT_DIR para testar com XLSX já exportados "
            "ou implemente o login/download real em backend/app/collector.py."
        )


def build_collector() -> IseqCollector:
    token = os.getenv("ISEQ_BEARER_TOKEN")
    if token:
        return ApiReportCollector(token=token, api_base=os.getenv("ISEQ_API_BASE"))
    export_dir = os.getenv("ISEQ_EXPORT_DIR")
    if export_dir:
        return LocalExportCollector(export_dir)
    return NotConfiguredCollector()


def build_export_tasks(equipment_id: str, start: datetime, end: datetime) -> list[ExportTask]:
    tasks: list[ExportTask] = []
    chunk_days = max(1, int(os.getenv("ISEQ_CHUNK_DAYS", "1")))
    for chunk_start, chunk_end in day_chunks(start, end, chunk_days):
        for parameter in KNOWN_PARAMETERS:
            tasks.append(ExportTask(equipment_id=equipment_id, parameter=parameter, start=chunk_start, end=chunk_end))
    return tasks


def day_chunks(start: datetime, end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    if end < start:
        raise ValueError("end must be after start")

    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=days))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
        if cursor == end:
            break
    return chunks

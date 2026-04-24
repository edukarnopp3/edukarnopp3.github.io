from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import shutil
import time

from .iseq_parser import KNOWN_PARAMETERS, month_chunks, normalize_parameter


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


class LocalExportCollector(IseqCollector):
    """Uses already downloaded ISEQ XLSX files.

    Set ISEQ_EXPORT_DIR to a directory containing files like
    CO2_1C_69_20_C7_31_D8_2026-04-24T20-35.xlsx. This is useful while the
    authenticated website automation is not wired yet.
    """

    def __init__(self, export_dir: str | os.PathLike[str]):
        self.export_dir = Path(export_dir)

    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        candidates = []
        equipment_token = task.equipment_id.replace(":", "_").lower()
        for path in self.export_dir.glob("*.xlsx"):
            parameter = normalize_parameter(path.stem.split("_", 1)[0])
            if parameter != task.parameter:
                continue
            normalized_name = path.name.lower().replace(".", "")
            if equipment_token not in normalized_name:
                continue
            candidates.append(path)

        if not candidates:
            raise FileNotFoundError(f"Nenhum XLSX local encontrado para {task.parameter} em {self.export_dir}.")

        source = max(candidates, key=lambda p: p.stat().st_mtime)
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / f"{task.parameter.replace('.', '_')}_{int(time.time())}.xlsx"
        shutil.copy2(source, destination)
        return destination


class NotConfiguredCollector(IseqCollector):
    def fetch_export(self, task: ExportTask, destination_dir: Path) -> Path:
        raise CollectorNotConfigured(
            "Coletor ISEQ não configurado. Configure ISEQ_EXPORT_DIR para testar com XLSX já exportados "
            "ou implemente o login/download real em backend/app/collector.py."
        )


def build_collector() -> IseqCollector:
    export_dir = os.getenv("ISEQ_EXPORT_DIR")
    if export_dir:
        return LocalExportCollector(export_dir)
    return NotConfiguredCollector()


def build_export_tasks(equipment_id: str, start: datetime, end: datetime) -> list[ExportTask]:
    tasks: list[ExportTask] = []
    for parameter in KNOWN_PARAMETERS:
        for chunk_start, chunk_end in month_chunks(start, end):
            tasks.append(ExportTask(equipment_id=equipment_id, parameter=parameter, start=chunk_start, end=chunk_end))
    return tasks

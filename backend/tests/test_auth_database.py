from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.collector import IseqCollector, build_export_tasks
from app.database import DatabaseStore, IseqConnection
from app.jobs import JobState, JobStore, TaskState
import app.main as main_module


APP_SECRET = "test-secret-with-at-least-thirty-two-characters"


class FakeCollector(IseqCollector):
    def list_equipment(self) -> list[dict[str, str]]:
        return [
            {
                "mac": "AA:BB:CC:DD:EE:01",
                "location": "Sala de testes",
                "label": "Sala de testes (AA:BB:CC:DD:EE:01)",
            }
        ]


class DatabaseStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "test.db"
        self.database = DatabaseStore(
            database_url=f"sqlite:///{db_path.as_posix()}",
            app_secret=APP_SECRET,
            storage_dir=self.temp_dir.name,
        )

    def tearDown(self) -> None:
        self.database.engine.dispose()
        self.temp_dir.cleanup()

    def test_token_session_sensors_jobs_and_readings_are_persisted(self) -> None:
        user = self.database.connect_iseq_account(
            login_hint="pesquisador@example.com",
            token="iseq-secret-token",
            profile={"id": 71, "username": "pesquisador", "email": "pesquisador@example.com"},
        )
        user_id = str(user["id"])

        with Session(self.database.engine) as session:
            connection = session.get(IseqConnection, user_id)
            self.assertIsNotNone(connection)
            self.assertNotEqual(connection.encrypted_token, "iseq-secret-token")
            self.assertNotIn("iseq-secret-token", connection.encrypted_token)
        self.assertEqual(self.database.get_iseq_token(user_id), "iseq-secret-token")

        raw_session = self.database.create_session(user_id, hours=12)
        context = self.database.get_session(raw_session)
        self.assertIsNotNone(context)
        self.assertEqual(context.user_id, user_id)

        equipment = FakeCollector().list_equipment()
        self.database.save_sensors(user_id, equipment)
        self.assertTrue(self.database.user_has_sensor(user_id, equipment[0]["mac"]))

        state = {
            "id": "job-test-1",
            "user_id": user_id,
            "equipment_id": equipment[0]["mac"],
            "start": "2026-03-01T00:00:00",
            "end": "2026-03-01T01:00:00",
            "status": "completed",
            "created_at": "2026-03-01T00:00:00",
            "updated_at": "2026-03-01T01:00:00",
            "total_tasks": 9,
            "worker_count": 3,
            "completed_tasks": 9,
            "attempted_tasks": 9,
            "failed_attempts": 0,
            "message": "Concluído.",
            "tasks": [],
            "data_file": None,
        }
        self.database.save_import_job(state)
        self.assertEqual(self.database.load_import_job("job-test-1")["user_id"], user_id)

        self.database.save_readings(
            user_id,
            equipment[0]["mac"],
            "job-test-1",
            [
                {
                    "data_local": "2026-03-01T00:00:13",
                    "CO2": 478,
                    "NOx": 12.5,
                    "VOC": 100,
                    "PM10": 4,
                    "PM2.5": 3,
                    "PM1": 2,
                    "Umid": 63.3,
                    "Temp": 27.6,
                    "Pressao": 1015.4,
                }
            ],
        )
        rows = self.database.get_readings(
            user_id,
            equipment[0]["mac"],
            datetime(2026, 3, 1),
            datetime(2026, 3, 2),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["CO2"], 478.0)
        self.assertEqual(rows[0]["PM2.5"], 3.0)

        self.database.save_readings(
            user_id,
            equipment[0]["mac"],
            "job-test-1",
            [{"data_local": "2026-03-01T00:00:13", "CO2": 500}],
        )
        updated_rows = self.database.get_readings(
            user_id,
            equipment[0]["mac"],
            datetime(2026, 3, 1),
            datetime(2026, 3, 2),
        )
        self.assertEqual(len(updated_rows), 1)
        self.assertEqual(updated_rows[0]["CO2"], 500.0)
        self.assertEqual(updated_rows[0]["PM2.5"], 3.0)

        self.database.revoke_session(raw_session)
        self.assertIsNone(self.database.get_session(raw_session))

    def test_coverage_is_isolated_by_sensor(self) -> None:
        user = self.database.connect_iseq_account(
            login_hint="cobertura@example.com",
            token="iseq-coverage-token",
            profile={"id": 72, "username": "cobertura"},
        )
        user_id = str(user["id"])
        start = datetime(2026, 3, 1)
        middle = datetime(2026, 3, 1, 12)
        end = datetime(2026, 3, 2)

        self.database.save_coverage(
            user_id,
            "AA:BB:CC:DD:EE:01",
            "coverage-job",
            [
                {"parameter": "CO2", "start": start, "end": middle},
                {"parameter": "CO2", "start": middle, "end": end},
            ],
        )

        coverage = self.database.get_coverage(
            user_id,
            "AA:BB:CC:DD:EE:01",
            start,
            end,
        )
        self.assertEqual(coverage["CO2"], [(start, middle), (middle, end)])
        self.assertEqual(
            self.database.get_coverage(
                user_id,
                "AA:BB:CC:DD:EE:02",
                start,
                end,
            ),
            {},
        )

    def test_completed_period_is_loaded_without_calling_iseq(self) -> None:
        user = self.database.connect_iseq_account(
            login_hint="cache@example.com",
            token="iseq-cache-token",
            profile={"id": 73, "username": "cache"},
        )
        user_id = str(user["id"])
        equipment_id = "AA:BB:CC:DD:EE:01"
        start = datetime(2026, 3, 1)
        end = datetime(2026, 3, 1, 1)
        tasks = build_export_tasks(equipment_id, start, end)

        self.database.save_readings(
            user_id,
            equipment_id,
            "original-job",
            [{"data_local": "2026-03-01T00:30:00", "CO2": 480}],
        )
        self.database.save_coverage(
            user_id,
            equipment_id,
            "original-job",
            [
                {
                    "parameter": task.parameter,
                    "start": task.start,
                    "end": task.end,
                }
                for task in tasks
            ],
        )

        def unexpected_collector(_user_id: str) -> IseqCollector:
            raise AssertionError("A ISEQ não deveria ser chamada para um período em cache.")

        store = JobStore(
            storage_dir=Path(self.temp_dir.name) / "cached-jobs",
            collector_factory=unexpected_collector,
            repository=self.database,
        )
        job = store.create_job(
            equipment_id,
            start,
            end,
            workers=6,
            user_id=user_id,
        )

        self.assertEqual(job.status, "completed")
        self.assertEqual(job.cached_tasks, len(tasks))
        self.assertEqual(job.completed_tasks, len(tasks))
        self.assertEqual(job.download_tasks, 0)
        self.assertTrue(all(task.status == "cached" for task in job.tasks))
        self.assertEqual(store.get_data(job.id)[0]["CO2"], 480.0)


class AuthApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "api.db"
        self.database = DatabaseStore(
            database_url=f"sqlite:///{db_path.as_posix()}",
            app_secret=APP_SECRET,
            storage_dir=self.temp_dir.name,
        )
        self.store = JobStore(
            storage_dir=Path(self.temp_dir.name) / "jobs",
            collector_factory=lambda _user_id: FakeCollector(),
            repository=self.database,
        )
        self.original_database = main_module.database
        self.original_store = main_module.store
        main_module.database = self.database
        main_module.store = self.store
        main_module.login_limiter.failures.clear()
        self.client = TestClient(main_module.app)

    def tearDown(self) -> None:
        self.client.close()
        main_module.database = self.original_database
        main_module.store = self.original_store
        self.database.engine.dispose()
        self.temp_dir.cleanup()

    def login(self, profile_id: int = 90) -> str:
        with patch.object(
            main_module,
            "authenticate_iseq",
            return_value=(
                f"iseq-token-{profile_id}",
                {"id": profile_id, "username": f"usuario-{profile_id}"},
            ),
        ):
            response = self.client.post(
                "/api/auth/iseq/login",
                json={"username_or_email": f"usuario-{profile_id}", "password": "senha-secreta"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["session_token"]

    def test_login_session_equipment_and_logout(self) -> None:
        token = self.login()
        headers = {"Authorization": f"Bearer {token}"}

        session_response = self.client.get("/api/auth/session", headers=headers)
        self.assertEqual(session_response.status_code, 200)
        self.assertEqual(session_response.json()["user"]["username"], "usuario-90")

        equipment_response = self.client.get("/api/iseq/equipment", headers=headers)
        self.assertEqual(equipment_response.status_code, 200)
        self.assertEqual(len(equipment_response.json()["equipment"]), 1)

        logout_response = self.client.post("/api/auth/logout", headers=headers)
        self.assertEqual(logout_response.status_code, 200)
        expired_response = self.client.get("/api/auth/session", headers=headers)
        self.assertEqual(expired_response.status_code, 401)
        self.assertEqual(expired_response.json()["detail"]["code"], "session_invalid")

    def test_jobs_are_isolated_by_user(self) -> None:
        first_token = self.login(profile_id=101)
        second_token = self.login(profile_id=202)
        first_context = self.database.get_session(first_token)
        self.assertIsNotNone(first_context)
        self.store.jobs["private-job"] = JobState(
            id="private-job",
            user_id=first_context.user_id,
            equipment_id="AA:BB:CC:DD:EE:01",
            start="2026-03-01T00:00:00",
            end="2026-03-01T01:00:00",
        )

        response = self.client.get(
            "/api/iseq/jobs/private-job",
            headers={"Authorization": f"Bearer {second_token}"},
        )
        self.assertEqual(response.status_code, 404)

    def test_protected_endpoint_rejects_anonymous_request(self) -> None:
        response = self.client.get("/api/iseq/equipment")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"]["code"], "session_invalid")

    def test_hosted_login_requires_persistent_database(self) -> None:
        with patch.dict("os.environ", {"RENDER": "true"}):
            response = self.client.post(
                "/api/auth/iseq/login",
                json={"username_or_email": "usuario", "password": "senha"},
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"]["code"], "database_not_configured")


class JobAuthenticationFailureTests(unittest.TestCase):
    def test_expired_iseq_token_stops_job_without_retrying(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(storage_dir=tmp)
            task = TaskState(
                equipment_id="AA:BB:CC:DD:EE:01",
                parameter="CO2",
                start="2026-03-01T00:00:00",
                end="2026-03-02T00:00:00",
                status="running",
                attempts=1,
            )
            job = JobState(
                id="expired-token-job",
                equipment_id=task.equipment_id,
                start=task.start,
                end=task.end,
                tasks=[task],
                total_tasks=1,
            )
            store.jobs[job.id] = job
            store._record_retry(job.id, task, RuntimeError("ISEQ API HTTP 403: Token inválido"))

            self.assertEqual(job.status, "failed")
            self.assertEqual(task.status, "failed")
            self.assertIsNone(task.next_retry_at)


if __name__ == "__main__":
    unittest.main()

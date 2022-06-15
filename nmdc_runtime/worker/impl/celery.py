from uuid import UUID
from typing import Any

from celery.app import Celery
from celery.result import AsyncResult

from nmdc_runtime.worker.task_manager import TaskManager


class CeleryManager(TaskManager):
    def connect_to_task_manager(self) -> None:
        self.client = Celery(
            __name__,
            __broker="redis://localhost:6379/0",
            backend="redis://localhost:6379/0",
        )

    def get_task_status(self, task_id: UUID) -> str:
        try:
            response = AsyncResult(task_id)
            return response.status
        except Exception:
            return "error"

    def get_task_result(self, task_id: UUID) -> Any:
        res = AsyncResult(task_id)
        return res.get()

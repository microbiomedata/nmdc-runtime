from nmdc_runtime.task.task_manager import TaskManager
from nmdc_runtime.task.impl.celery import CeleryManager

tm = CeleryManager()


def get_task_manager() -> TaskManager:
    return tm

from uuid import UUID

from fastapi import api_router

from nmdc_runtime.models.task import TaskInit, Task, Tasks, TaskView


router = APIRouter()


@router.post("", status_code=201, response_model=UUID)
async def post_task(
    data_in: TaskInit
) -> UUID:
"""
Create a new task.
"""
pass


@router.get("/tasks", status_code=200, response_model=TaskList)
async def list_tasks(
    name_prefix: Optional[str] = None,
    page_size: int = 256,
    page_token: int = 1,
    view: TaskView = TaskView.minimal
) -> TaskList:
"""
Fetch list of tasks.
"""
pass


@router.get("/tasks/{task_id}", status_code=200, response_model=Task)
async def get_task(
    task_id: UUID,
    view: TaskView = TaskView.minimal
) -> Task:
"""
Fetch Task.
"""
pass


@router.delete("/tasks/{task_id}", status_code=202):
async def cancel_task(task_id: UUID) -> None:
    """
    Cancel Task.
    """
    pass

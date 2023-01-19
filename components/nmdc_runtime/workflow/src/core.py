from .spec import ResultWithErr, WorkflowQueriesABC, WorkflowUpdate
from .store import WorkflowQueries


class WorkflowService:
    def __init__(
        self, workflow_queries: WorkflowQueriesABC = WorkflowQueries()
    ) -> None:
        self.__queries = workflow_queries

    async def update(self, workflow: WorkflowUpdate) -> ResultWithErr[dict]:
        result = await self.__queries.workflow_update(workflow)
        return {"data": result}, None


def init_workflow_service() -> WorkflowService:
    return WorkflowService()

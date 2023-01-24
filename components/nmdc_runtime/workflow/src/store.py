from typing import TYPE_CHECKING, Optional

from beanie import Document, Indexed, Link
from beanie.operators import Push
from pymongo import TEXT, IndexModel
from pymongo.errors import DuplicateKeyError

from .spec import Workflow, WorkflowCreate, WorkflowQueriesABC, WorkflowUpdate


class WorkflowCurrent(Document, Workflow):
    @classmethod
    async def by_name(cls, *, name: str) -> Optional["WorkflowCurrent"]:
        return await cls.find_one(cls.name == name)

    class Settings:
        name = "workflow_current"
        use_revision = True
        indexes = [
            IndexModel([("name", TEXT)], unique=True),
        ]


class WorkflowRevision(Document):
    if TYPE_CHECKING:
        name: str
    else:
        name: Indexed(str, unique=True)
    revisions: list[Workflow]
    current: Link[WorkflowCurrent]

    class Settings:
        name = "workflow_revisions"


class WorkflowQueries:
    async def workflow_create(self, creation: WorkflowCreate) -> str:
        try:
            workflow = WorkflowCurrent(**dict(creation))

            revision = WorkflowRevision(
                name=creation["name"], revisions=[], current=workflow
            )

            await workflow.save()
            await revision.save()

            return f"{creation['name']} added to the collection"

        except DuplicateKeyError as e:
            raise e

    async def workflow_update(self, update: WorkflowUpdate) -> str:
        try:
            workflow_old = await WorkflowCurrent.find_one(
                WorkflowCurrent.name == update["name"]
            )
            if not workflow_old:
                return f"{update['name']} doesn't exist"

            await WorkflowRevision.find_one(
                WorkflowRevision.name == workflow_old.name
            ).update(Push({WorkflowRevision.revisions: workflow_old.dict()}))

            await workflow_old.set(dict(update))

            return f"{update['name']} updated to latest revision"

        except Exception as e:
            raise e

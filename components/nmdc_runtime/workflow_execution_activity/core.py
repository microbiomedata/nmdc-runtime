"""Core functionality of the activity service module."""
from dataclasses import fields
from typing import Any, TypedDict
from uuid import uuid1

from motor.motor_asyncio import AsyncIOMotorDatabase
from nmdc_schema.nmdc import Database, DataObject, WorkflowExecutionActivity

from components.nmdc_runtime.workflow import Workflow, WorkflowModel, get_all_workflows
from .store import MongoDatabase, insert_activities
from .spec import ActivityTree


class ActiveActivities(TypedDict):
    activities: list[WorkflowExecutionActivity]
    workflow: Workflow


class ActivityWithWorkflow(TypedDict):
    activity: WorkflowExecutionActivity
    workflow: Workflow


def get_active_activities(
    activities: Database,
) -> list[ActiveActivities]:
    activity_fields = fields(activities)
    active_activities: list[ActiveActivities] = []
    for field in activity_fields:
        if activities[field.name] and field.name != "data_object_set":
            active_activities.append(
                {
                    "activities": activities[field.name],
                    "workflow": WorkflowModel.parse_obj(
                        {"workflow": {"activity": field.name}}
                    ).workflow,
                }
            )

    return active_activities


def build_activity_trees(activities: list[ActiveActivities]) -> dict[str, ActivityTree]:
    """Create a list of activities and their children."""
    activity_trees: dict[str, ActivityTree] = {}
    raw_activities = []
    for activity in activities:
        for sub_activity in activity["activities"]:
            activity_trees[sub_activity.id] = ActivityTree(
                data=sub_activity, spec=activity["workflow"]
            )
            raw_activities.append(
                ActivityTree(data=sub_activity, spec=activity["workflow"])
            )

    for tree in activity_trees.values():
        for raw_activity in raw_activities:
            intersect = [
                data_object
                for data_object in tree.data.has_output
                if data_object in raw_activity.data.has_input
            ]
            if len(intersect) > 0:
                activity_trees[tree.data.id].children.append(raw_activity)
    return activity_trees


def is_child_p(workflow: Workflow, children: list[ActivityTree]) -> bool:
    """Predicate to determine if workflow already has an associated activity."""
    child_names: list[str] = [child.spec.name for child in children]

    if workflow.name in child_names:
        return True
    return False


def find_next_workflows(trees: dict[str, ActivityTree]) -> list[Workflow]:
    """Find next workflows to be run in sequence."""
    workflows: list[Workflow] = get_all_workflows()
    next_workflows: list[Workflow] = []
    for workflow in workflows:
        for tree in trees.values():
            if workflow.predecessor == tree.spec.name and not is_child_p(
                workflow, tree.children
            ):
                workflow_copy = workflow.copy()
                workflow_copy.inputs.proj = tree.data.id
                workflow_copy.inputs.informed_by = tree.data.was_informed_by
                next_workflows.append(workflow_copy)
    return next_workflows


def insert_into_keys(
    workflow: Workflow, data_objects: list[DataObject]
) -> dict[str, Any]:
    """Insert data object url into correct workflow input field."""
    workflow_dict = workflow.model_dump()
    for key in workflow_dict["inputs"]:
        for do in data_objects:
            if workflow_dict["inputs"][key] == str(do.data_object_type):
                workflow_dict["inputs"][key] = str(do.url)
    return workflow_dict


def populate_workflow_objects(
    workflows: list[Workflow],
    activity_trees: dict[str, ActivityTree],
    data_objects: list[DataObject],
) -> list[dict[str, Any]]:
    """Populate workflow objects with missing data."""
    workflow_list = []
    for workflow in workflows:
        inputs = activity_trees[workflow.inputs.proj].data.has_output
        intersect: list[DataObject] = [
            data_object for data_object in data_objects if data_object.id in inputs
        ]
        workflow_list.append(insert_into_keys(workflow, intersect))
    return workflow_list


class ActivityService:
    """Methods for creation and insertion of NMDC Workflow Execution Activities."""

    async def create_jobs(
        self,
        activities: list[ActiveActivities],
        data_objects: list[DataObject],
        mdb: AsyncIOMotorDatabase,
    ) -> bool:
        """Create jobs for automation.

        Parameters
        ----------
        activities : list[ActiveActivities]
           Beans.
        data_objects : list[DataObject]

        Returns
        -------
        list[dict[str,Any]]
        """
        trees = build_activity_trees(activities)
        next_workflows = find_next_workflows(trees)
        job_configs = populate_workflow_objects(next_workflows, trees, data_objects)

        for job in job_configs:
            job_spec = {
                "id": f"sys:test_{str(uuid1())}",
                "workflow": {"id": f"{job['id_type']}-{job['version']}"},
                "config": {**job},
                "claims": [],
            }
            await mdb["jobs"].insert_one(job_spec)

        return True

    async def add_activity_set(
        self, activities: Database, db: MongoDatabase
    ) -> list[ActiveActivities]:
        """
        Store workflow activities.

        Parameters
        ----------
        activities : Database
            dictionary of fields for data object creation

        db : A database
            service for interacting with data objects

        Returns
        -------
        list[ActiveActivities]
            IDs for all activities added to the collection
        """
        insert_activities(activities, db)
        active_activities = get_active_activities(activities)
        return active_activities


def init_activity_service() -> ActivityService:
    """
    Instantiate an activity service.

    Returns
    -------
    ActivityService
    """
    return ActivityService()

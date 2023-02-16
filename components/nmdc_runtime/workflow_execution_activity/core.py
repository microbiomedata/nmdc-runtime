"""Core functionality of the activity service module."""
from dataclasses import fields
from typing import Any, TypedDict
from uuid import uuid1

from motor.motor_asyncio import AsyncIOMotorDatabase
from nmdc_schema.nmdc import Database, DataObject, WorkflowExecutionActivity

from components.nmdc_runtime.workflow import Workflow, WorkflowModel, get_all_workflows
from .store import MongoDatabase, insert_activities


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


def add_relevant_info(
    workflow: Workflow, activity: WorkflowExecutionActivity
) -> Workflow:
    workflow.inputs.proj = activity.id
    workflow.inputs.informed_by = activity.was_informed_by
    return workflow


def construct_job_config(
    activity: WorkflowExecutionActivity, name: str
) -> list[Workflow]:
    workflows = get_all_workflows()
    next_workflows = list(filter(lambda wf: wf.predecessor == name, workflows))
    relevant_info = [add_relevant_info(wf, activity) for wf in next_workflows]
    return relevant_info


def container_job(
    activities: list[WorkflowExecutionActivity], name: str
) -> list[list[Workflow]]:
    return [construct_job_config(activity, name) for activity in activities]


def parse_data_objects(
    activity: Workflow, data_objects: list[DataObject]
) -> dict[str, Any]:
    activity_dict = activity.dict()
    for key in activity_dict["inputs"]:
        for do in data_objects:
            if activity_dict["inputs"][key] == str(do.data_object_type):
                activity_dict["inputs"][key] = str(do.url)  # I'm very upset about this

    return activity_dict


def associate_activity_with_workflow(
    aa: ActiveActivities,
) -> list[ActivityWithWorkflow]:
    return [
        {"activity": activity, "workflow": aa["workflow"]}
        for activity in aa["activities"]
    ]


def get_input_set(activities: list[ActivityWithWorkflow]) -> set[str]:
    activity_input_set = set()
    for entry in activities:
        activity_input_set.update(entry["activity"].has_input)
    return activity_input_set


def outputs_in_inputs(activity: ActivityWithWorkflow, inputs: set[str]) -> bool:
    for output in activity["activity"].has_outputs:
        if output in inputs:
            return True

    return False


def filter_activities(
    activities: list[ActivityWithWorkflow], inputs: set[str]
) -> list[ActivityWithWorkflow]:
    leaves: list[ActivityWithWorkflow] = []
    for activity in activities:
        if not outputs_in_inputs(activity, inputs):
            leaves.append(activity)

    return leaves


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
        flatten = lambda *n: (
            e
            for a in n
            for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,))
        )
        flattened_activities: list[ActivityWithWorkflow] = list(
            flatten([associate_activity_with_workflow(entry) for entry in activities])
        )
        input_set = get_input_set(flattened_activities)
        job_configs: list[dict[str, Any]] = [
            parse_data_objects(activity["workflow"], data_objects)
            for activity in filter_activities(flattened_activities, input_set)
        ]
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

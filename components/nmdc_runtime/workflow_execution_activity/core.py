"""Core functionality of the activity service module."""
from dataclasses import fields
from typing import Any, TypedDict

from nmdc_schema.nmdc import Database, DataObject, WorkflowExecutionActivity

from components.nmdc_runtime.workflow import Workflow, WorkflowModel, get_all_workflows


from .store import MongoDatabase, insert_activities


class ActiveActivities(TypedDict):
    activities: list[WorkflowExecutionActivity]
    workflow: Workflow


flatten = lambda *n: (
    e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,))
)


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


def construct_job_config(activity: WorkflowExecutionActivity, name: str) -> Any:
    workflows = get_all_workflows()
    next_workflows = list(filter(lambda wf: wf.predecessor == name, workflows))
    relevant_info = [add_relevant_info(wf, activity) for wf in next_workflows]
    return relevant_info


def container_job(
    activities: list[WorkflowExecutionActivity], name: str
) -> list[Workflow]:
    jobs = [construct_job_config(activity, name) for activity in activities]
    return jobs


def parse_data_objects(
    activity: WorkflowExecutionActivity, data_objects: list[DataObject]
) -> Workflow:
    for key in activity.inputs:
        for do in data_objects:
            if activity.inputs[key] == str(do.data_object_type):
                activity.inputs["inputs"][key] = str(
                    do.url
                )  # I'm very upset about this

    return activity.dict()


class ActivityService:
    def create_jobs(
        self,
        activities: list[ActiveActivities],
        data_objects: list[DataObject],
    ) -> list[WorkflowExecutionActivity]:
        """Create jobs for automation.

        Parameters
        ----------
        activities : list[ActiveActivities]
           Beans.
        data_objects : list[DataObject]

        Returns
        -------
        list[Workflow]
        """
        processed_activities: list[Workflow] = list(
            flatten(
                [
                    container_job(aa["activities"], aa["workflow"].name)
                    for aa in activities
                ]
            )
        )
        return [
            parse_data_objects(activity, data_objects)
            for activity in processed_activities
        ]

    async def add_activity_set(
        self, activities: Database, db: MongoDatabase
    ) -> list[WorkflowExecutionActivity]:
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
        list[str]
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

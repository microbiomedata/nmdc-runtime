"""Core functionality of the activity service module."""
import functools
import json
import logging
import operator
from dataclasses import Field, fields
from typing import Any, Dict, TypedDict

from beanie import Document
from components.nmdc_runtime.workflow.spec import WorkflowModel, get_all_workflows
from nmdc_schema.nmdc import Database, DataObject, WorkflowExecutionActivity

from .store import insert_activities


class ActiveActivities(TypedDict):
    activities: list[WorkflowExecutionActivity]
    workflow: WorkflowModel


flatten = lambda *n: (
    e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,))
)


def get_active_activities(
    activities: Database,
) -> list[ActiveActivities]:
    activity_fields: tuple[Field[Database.activity_set]] = fields(activities)
    active_activities: list[ActiveActivities] = []
    for field in activity_fields:
        if activities[field.name] and field.name != "data_object_set":
            active_activities.append(
                {
                    "activities": activities[field.name],
                    "workflow": WorkflowModel(workflow={"activity": field.name}),
                }
            )

    return active_activities


def add_relevant_info(workflow, activity):
    workflow.inputs.proj = activity.id
    workflow.inputs.informed_by = activity.was_informed_by
    return workflow


def construct_job_config(
    activity: WorkflowExecutionActivity, name: str
) -> WorkflowModel:
    workflows = get_all_workflows()
    next_workflows = list(filter(lambda wf: wf.predecessor == name, workflows))
    relevant_info = [add_relevant_info(wf, activity) for wf in next_workflows]
    return relevant_info


def container_job(activities, name):
    jobs = [construct_job_config(activity, name) for activity in activities]
    return jobs


def parse_data_objects(activity, data_objects: list[DataObject]):
    new_activity = activity.dict()
    for key in new_activity["inputs"]:
        for do in data_objects:
            if new_activity["inputs"][key] == str(do.data_object_type):
                new_activity["inputs"][key] = str(do.url)  # I'm very upset about this

    return new_activity


class ActivityService:
    """Repository for interacting with nmdc workflow execution activities."""

    def create_jobs(self, activities, data_objects):
        processed_activities = list(
            flatten(
                [
                    container_job(aa["activities"], aa["workflow"].workflow.name)
                    for aa in activities
                ]
            )
        )
        return [
            parse_data_objects(activity, data_objects)
            for activity in processed_activities
        ]

    async def add_activity_set(self, activities: Database, db):
        """
        Store workflow activities.

        Parameters
        ----------
        activities : Database
            dictionary of fields for data object creation

        db: A database
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

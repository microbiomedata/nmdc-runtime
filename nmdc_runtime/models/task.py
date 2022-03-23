from datetime import datetime
from typing import Enum, List, Dict
from uuid import UUID

from pydantic import BaseModel, AnyUrl, DirectoryPath, Optional

class TaskView(str, Enum):
    minimal = "MINIMAL"
    basic = "BASIC"
    full = "FULL"


class TaskPathTypeEnum(str, Enum):
    file = "FILE"
    directory = "DIRECTORY"


class TaskInput(BaseModel):
    path: DirectoryPath
    url: Optional[AnyUrl]
    type: TaskPathTypeEnum = TaskPathTypeEnum.file


class TaskOutput(BaseModel):
    name: Optional[str]
    description: Optional[str]
    url: AnyUrl
    path: DirectoryPath
    type: TaskPathTypeEnum = TaskPathTypeEnum.file


class TaskExecutor(BaseModel):
    image: str
    command: str


class TaskResources(BaseModel):
    cpu_cores: Optional[int]
    preemptible: Optional[bool]
    ram_gb: Optional[int]
    disk_gb: Optional[int]
    zones: Optional[str]


class Task(BaseModel):
    task_id: UUID
    state: str
    name: Optional[str]
    description: Optional[str]
    inputs: Optional[List[TaskInput]]
    outputs: Optional[List[TaskOutput]]
    resources: Optional[TaskResources]
    executors: Optional[TaskExecutors]
    volumes: Optional[List[DirectoryPath]]
    tags: Optioanl[Dict[str, str]]
    creation_time: Optional[datetime]
    

class TaskList(BaseModel):
    tasks: List[Task]
    next_page_token: int = 1
      
class TaskInit(BaseModel):
    name: Optional[str]
    description: Optional[str]
    inputs: List[TaskInput]
    outputs: List[TaskOutput]
    executors: List[TaskExecutor]
    resources: Optional[TaskResources]
    volumes: Optional[List[DirectoryPath]]
    tags: Optional[Dict[str, str]]

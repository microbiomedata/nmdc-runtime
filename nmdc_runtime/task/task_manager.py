from abc import abstractmethod
from uuid import UUID


class TaskManager(object):
    @property
    def client(self):
        raise NotImplementedError

    @property
    def manager(self):
        raise NotImplementedError

    @abstractmethod
    def connect_to_task_manager(self):
        pass

    @abstractmethod
    def create_task(self, task_input) -> UUID:
        pass

    @abstractmethod
    def get_task_status(self, task_id: UUID) -> str:
        pass

    @abstractmethod
    def get_task_result(self, task_id: UUID):
        pass

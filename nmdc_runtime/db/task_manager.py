from abc import abstractmethod


class TaskManager(object):
    @property
    def client(self):
        raise NotImplementedError

    @property
    def service(self):
        raise NotImplementedError

    @abstractmethod
    async def connect_to_task_manager(self, path: str):
        pass

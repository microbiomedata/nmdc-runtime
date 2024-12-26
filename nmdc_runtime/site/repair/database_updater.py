from nmdc_runtime.site.resources import RuntimeApiUserClient
from nmdc_schema import nmdc


class DatabaseUpdater:
    def __init__(self, runtime_api_user_client: RuntimeApiUserClient, study_id: str):
        self.runtime_api_user_client = runtime_api_user_client
        self.study_id = study_id

    def create_missing_dg_records(self):
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()
        self.create_missing_dg_records()
        return database

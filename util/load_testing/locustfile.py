r"""
Note: This is a [Locustfile](https://docs.locust.io/en/stable/writing-a-locustfile.html),
      which is a Python module from which Locust (the load testing tool) will read tasks.
      Tasks are the things that the "simulated users" will perform in order to place
      a load on the system under test (SUT). In this case, the SUT is the Runtime API.
"""

from locust import HttpUser, task


class SomeUser(HttpUser):
    @task
    def get_version(self):
        """
        A baseline task that doesn't involve authentication or MongoDB.
        """
        self.client.get("/version")

    @task
    def get_biosamples(self):
        """
        A task that involves querying a large MongoDB database
        via a collection-specific endpoint.
        """
        self.client.get("/biosamples")

    @task
    def get_biosample_set_documents(self):
        """
        A task that involves querying a large MongoDB collection
        via a general-purpose endpoint.
        """
        self.client.get("/nmdcschema/biosample_set")

    @task
    def get_study_set_documents(self):
        """
        A task that involves querying a small MongoDB collection
        via a general-purpose endpoint.
        """
        self.client.get("/nmdcschema/study_set")

r"""
Note: This is a [Locustfile](https://docs.locust.io/en/stable/writing-a-locustfile.html),
      which is a Python module from which Locust (the load testing tool) will read tasks.
      Tasks are the things that the "simulated users" will perform in order to place
      a load on the system under test (SUT). In this case, the SUT is the Runtime API.
"""

import base64

from locust import HttpUser, task


# This is a quick-n-dirty way to specify user credentials for load testing,
# allowing for visual obfuscation (which can be undone via base64 decoding)
# for sharing screen with team members, mitigating accidental retention
# via casual glancing. THIS IS NOT SECURE. DO NOT SCREENSHOT THESE VALUES.
# 
# You can use the following website to generate base64-encoded strings of
# your username and password:
# https://emn178.github.io/online-tools/base64_encode.html
#
# FIXME: Get these from environment variables instead.
#
username = base64.b64decode("__REPLACE_ME__")
password = base64.b64decode("__REPLACE_ME__")


class User(HttpUser):

    def on_start(self):
        """
        Exchanges user credentials for a user access token, and then updates
        the client headers to include the user access token for subsequent requests.
        """

        response = self.client.post(
            "/token",
            data={
                "username": username,
                "password": password,
                "grant_type": "password",
            },
        )
        access_token = response.json()["access_token"]
        self.client.headers.update({"Authorization": f"Bearer {access_token}"})

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
    def get_data_object_set_documents(self):
        """
        A task that involves querying a medium MongoDB collection
        via a general-purpose endpoint.
        """
        self.client.get("/nmdcschema/data_object_set")

    @task
    def get_study_set_documents(self):
        """
        A task that involves querying a small MongoDB collection
        via a general-purpose endpoint.
        """
        self.client.get("/nmdcschema/study_set")

    @task
    def get_me(self):
        """
        A task that involves authentication.
        """
        self.client.get("/users/me")


class SiteClient(HttpUser):

    def on_start(self):
        """
        Exchanges user credentials for a user access token, generates site client credentials,
        exchanges those for a site client access token, and then updates the client headers to
        include the site client access token for subsequent requests.
        """

        # Get a user access token.
        response = self.client.post(
            "/token",
            data={
                "username": username,
                "password": password,
                "grant_type": "password",
            },
        )
        user_access_token = response.json()["access_token"]
        self.client.headers.update({"Authorization": f"Bearer {user_access_token}"})

        # Get the sites of which the user is an administrator.
        response = self.client.get("/users/me")
        site_names = response.json().get("site_admin", [])
        assert len(site_names) >= 1, "User is not an administrator of any site"

        # Generate site client credentials for that site.
        # TODO: What if the site name, itself, contains a colon?
        response = self.client.post(f"/sites/{site_names[0]}:generateCredentials")
        client_credentials = response.json()
        client_id = client_credentials["client_id"]
        client_secret = client_credentials["client_secret"]
        assert client_id and client_secret, "Failed to obtain site client credentials"

        # Exchange the site client credentials for a site client access token.
        # TODO: Write user-facing tutorial for submitting this request programmatically.
        #       It took me a while to realize the endpoint expected `client_credentials`
        #       (with an underscore) rather than `clientCredentials` (camelCase).
        response = self.client.post(
            "/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        site_client_access_token = response.json()["access_token"]
        self.client.headers.update({"Authorization": f"Bearer {site_client_access_token}"})

    @task
    def mint_data_object_ids(self):
        """
        A task that involves minting IDs.
        
        Caution: This'll grow the `minter.id_records` collection in whatever
                 MongoDB database the Runtime API is configured to use.
        """
        schema_class_descriptor = {"id": "nmdc:DataObject"}
        self.client.post(
            "/pids/mint",
            json={
                "how_many": 25,
                "schema_class": schema_class_descriptor,
            },
        )

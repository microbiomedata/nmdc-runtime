r"""
Note: This is a [Locustfile](https://docs.locust.io/en/stable/writing-a-locustfile.html),
      which is a Python module from which Locust (the load testing tool) will read tasks.
      Tasks are the things that the "simulated users" will perform in order to place
      a load on the system under test (SUT). In this case, the SUT is the Runtime API.
"""

import base64
import json

from locust import HttpUser, task, tag


# This is a quick-n-dirty way to specify user credentials for load testing,
# allowing for visual obfuscation (which can be undone via base64 decoding)
# for sharing screen with team members, mitigating accidental retention
# via casual glancing. THIS IS NOT SECURE. DO NOT SCREENSHOT THESE VALUES.
# 
# You can use the following website to generate base64-encoded strings of
# your username and password:
# https://emn178.github.io/online-tools/base64_encode.html
#
# FIXME: Get the decoded values from environment variables instead.
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
    def get_biosample_set_documents_with_id_filter(self):
        """
        A task that involves querying a large MongoDB collection,
        with an ID filter, via a general-purpose endpoint.
        """
        # Note: This is a random selection of `id` values from the
        #       `biosample_set` collection in the production database.
        biosample_ids = [
            "nmdc:bsm-11-s8qcv981",
            "nmdc:bsm-11-nk66eb88",
            "nmdc:bsm-12-zgd8pe61",
            "nmdc:bsm-11-q35t8p62",
            "nmdc:bsm-11-75ttqm18",
            "nmdc:bsm-11-cz68sy30",
            "nmdc:bsm-11-7z1qvf63",
            "nmdc:bsm-11-ym34cp57",
            "nmdc:bsm-11-pksg9b77",
            "nmdc:bsm-11-m4k42235",
            "nmdc:bsm-11-4jxw8910",
            "nmdc:bsm-11-8rn9bm20",
            "nmdc:bsm-11-0v1etz47",
            "nmdc:bsm-11-evx13f88",
            "nmdc:bsm-11-08fpzx54",
            "nmdc:bsm-11-x3ef2r26",
            "nmdc:bsm-11-gh6v0688",
            "nmdc:bsm-11-0hrvfj70",
            "nmdc:bsm-11-ks45g154",
            "nmdc:bsm-12-yp97z138",
            "nmdc:bsm-11-wjjkmr15",
            "nmdc:bsm-11-69g1rw32",
            "nmdc:bsm-11-tdt16s29",
            "nmdc:bsm-11-d6yrc334",
            "nmdc:bsm-11-e2ett693",
        ]
        filter_as_json: str = json.dumps({"id": {"$in": biosample_ids}})
        self.client.get(
            "/nmdcschema/biosample_set",
            params={"filter": filter_as_json},
            # Label the requests on the Locust web UI.
            # Note: We specify a `name` so that, regardless of the actual URL,
            #       the Locust web UI shows all requests using this specific name.
            name=r'/nmdcschema/biosample_set?filter={"id": {"$in": [...' + str(len(biosample_ids)) + r' elements...]}',
        )

    @task
    def get_data_object_set_documents(self):
        """
        A task that involves querying a medium MongoDB collection
        via a general-purpose endpoint.
        """
        self.client.get("/nmdcschema/data_object_set")

    @task
    def get_data_object_set_documents_with_regex_filter(self):
        """
        A task that involves querying a medium MongoDB collection,
        with a regex filter, via a general-purpose endpoint.
        """
        filter_as_json: str = json.dumps({"name": {"$regex": "^output"}})
        self.client.get(
            "/nmdcschema/data_object_set",
            params={"filter": filter_as_json},
            # Label the requests on the Locust web UI.
            name=r'/nmdcschema/data_object_set?filter=' + filter_as_json,
        )

    @task
    def get_data_object_set_documents_with_id_filter(self):
        """
        A task that involves querying a medium MongoDB collection,
        with an ID filter, via a general-purpose endpoint.
        """
        # Note: This is a random selection of `id` values from the
        #       `data_object_set` collection in the production database.
        data_object_ids = [
            "nmdc:dobj-11-hhc94a28",
            "nmdc:dobj-11-rx80r855",
            "nmdc:dobj-11-gwstdz46",
            "nmdc:dobj-11-zxzwtv46",
            "nmdc:dobj-11-4gx4sn35",
            "nmdc:dobj-11-ay61zq33",
            "nmdc:dobj-11-6r599366",
            "nmdc:dobj-11-zgbjaz16",
            "nmdc:dobj-11-8n5gy095",
            "nmdc:dobj-11-78zt0y84",
            "nmdc:dobj-11-m903xt06",
            "nmdc:dobj-11-d637r672",
            "nmdc:dobj-11-aek2vw66",
            "nmdc:dobj-11-vf47b635",
            "nmdc:dobj-11-h6v63484",
            "nmdc:dobj-11-w10f5a24",
            "nmdc:dobj-11-1c1j0809",
            "nmdc:dobj-11-j84wbz25",
            "nmdc:dobj-11-1x7xyv61",
            "nmdc:dobj-11-3x8kza53",
            "nmdc:dobj-11-0hzx4264",
            "nmdc:dobj-11-rpztnq73",
            "nmdc:dobj-11-0s51r362",
            "nmdc:dobj-11-6mt8dy96",
            "nmdc:dobj-11-bjbhte89",
        ]
        filter_as_json: str = json.dumps({"id": {"$in": data_object_ids}})
        self.client.get(
            "/nmdcschema/data_object_set",
            params={"filter": filter_as_json},
            # Label the requests on the Locust web UI.
            name=r'/nmdcschema/data_object_set?filter={"id": {"$in": [...' + str(len(data_object_ids)) + r' elements...]}',
        )

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
    def get_me(self):
        """
        A task that involves authentication.
        """
        self.client.get("/users/me")

    @tag("mints_ids")
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

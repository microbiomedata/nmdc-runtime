import os
import json
import requests

from dotenv import load_dotenv


class NMDCApiClient:
    def __init__(self, api_base_url=None):
        load_dotenv()
        self.base_url = api_base_url or os.getenv("API_HOST")
        if not self.base_url:
            raise ValueError("API base URL for runtime environment is required.")
        if not self.base_url.endswith("/"):
            self.base_url += "/"
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_biosamples_part_of_study(self, study_id: str) -> list[dict]:
        """
        Get the biosamples that are part of a study.
        """
        biosample_records = []
        params = {"filter": json.dumps({"part_of": study_id}), "max_page_size": "1000"}
        url = self.base_url + "nmdcschema/biosample_set"

        while True:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            biosample_records.extend(data["resources"])

            # Check if there's a next page
            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break
            params["page_token"] = next_page_token

        return biosample_records

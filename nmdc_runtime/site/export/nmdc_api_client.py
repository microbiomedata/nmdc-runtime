import requests


class NMDCApiClient:
    def __init__(self, api_base_url):
        if not api_base_url.endswith("/"):
            api_base_url += "/"
        self.base_url = api_base_url
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_biosamples_part_of_study(self, study_id: str) -> list[dict]:
        """
        Get the biosamples that are part of a study.
        """
        biosample_records = []
        params = {
            "filter": '{"part_of": "' + study_id + '"}',
            "max_page_size": "1000",
        }
        url = self.base_url + "nmdcschema/biosample_set"
        response = requests.get(url, params=params, headers=self.headers)
        response.raise_for_status()
        biosample_records.extend(response.json()["resources"])
        # Get the next page of results, if any
        while response.json().get("next_page_token") is not None:
            params["page_token"] = response.json()["next_page_token"]
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            biosample_records.extend(response.json()["resources"])

        return biosample_records

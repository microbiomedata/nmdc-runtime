from dagster import ConfigurableResource

from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiUserClient
import requests


class GoldApiResource(ConfigurableResource):
    """
    Resource for fetching GOLD biosamples
    """
    base_url: str
    username: str
    password: str

    def get_client(self) -> GoldApiClient:
        """
        Get a GOLD API client
        :return: GoldApiClient
        """
        client = GoldApiClient(
            base_url=self.base_url,
            username=self.username,
            password=self.password,
        )
        return client

class RuntimeApiUserResource(ConfigurableResource):
    """
    Resource wrapper for the runtime API user client
    """
    base_url: str
    username: str
    password: str

    def get_client(self) -> RuntimeApiUserClient:
        """
        Get a runtime API user client
        :return: RuntimeApiUserClient
        """
        client = RuntimeApiUserClient(
            base_url=self.base_url,
            username=self.username,
            password=self.password,
        )
        return client
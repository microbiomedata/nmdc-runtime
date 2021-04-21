from dagster import resource, Field, StringSource
from terminusdb_client import WOQLClient


class TerminusDB:
    def __init__(self, server_url, user, key, account, dbid):
        self.client = WOQLClient(server_url=server_url)
        self.client.connect(user=user, key=key, account=account)
        db_info = self.client.get_database(dbid=dbid, account=account)
        if db_info is None:
            self.client.create_database(dbid=dbid, accountid=account, label=dbid)
            self.client.create_graph(graph_type="inference", graph_id="main")
        self.client.connect(user=user, key=key, account=account, db=dbid)


@resource(
    config_schema={
        "server_url": StringSource,
        "user": StringSource,
        "key": StringSource,
        "account": StringSource,
        "dbid": StringSource,
    }
)
def terminus_resource(context):
    return TerminusDB(
        server_url=context.resource_config["server_url"],
        user=context.resource_config["user"],
        key=context.resource_config["key"],
        account=context.resource_config["account"],
        dbid=context.resource_config["dbid"],
    )

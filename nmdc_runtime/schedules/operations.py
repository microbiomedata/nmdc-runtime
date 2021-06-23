from dagster import schedule


# @schedule(
#     cron_schedule="* * * * *",
#     pipeline_name="create_object_from_site_object_put",
#     execution_timezone="America/Los_Angeles",
# )
# def create_object_from_site_object_put(_context):
#     # TODO properly config pipeline
#     run_config = {}
#     return run_config

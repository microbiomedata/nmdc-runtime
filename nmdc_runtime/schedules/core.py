from dagster import schedule

from nmdc_runtime.pipelines.core import housekeeping, preset_normal_env


def cron_schedule_definition_from_pipeline_preset(
    name, pipeline, preset, cron_schedule
):
    preset_name = preset.name
    if not preset:
        raise Exception(
            "Preset {preset_name} was not found "
            "on pipeline {pipeline_name}".format(
                preset_name=preset_name, pipeline_name=pipeline.name
            )
        )

    @schedule(
        name=name,
        cron_schedule=cron_schedule,
        execution_timezone="America/New_York",
        pipeline_name=pipeline.name,
        solid_selection=preset.solid_selection,
        mode=preset.mode,
        tags_fn=lambda _: preset.tags,
    )
    def my_schedule():
        return preset.run_config

    return my_schedule


housekeeping_weekly = cron_schedule_definition_from_pipeline_preset(
    "housekeeping_weekly", housekeeping, preset_normal_env, "45 6 * * 1"
)

"""
Dagster ops related to the ontology loader.

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

import logging
import os

from dagster import Noneable, OpExecutionContext, op, Field

from ontology_loader.ontology_load_controller import OntologyLoaderController


@op(
    required_resource_keys={"mongo"},
    config_schema={
        "source_ontology": str,
        "output_directory": Field(Noneable(str), default_value=None, is_required=False),
        "generate_reports": Field(bool, default_value=True, is_required=False),
    },
)
def load_ontology(context: OpExecutionContext):
    cfg = context.op_config
    source_ontology = cfg["source_ontology"]
    output_directory = cfg.get("output_directory")
    generate_reports = cfg.get("generate_reports", True)

    if output_directory is None:
        output_directory = os.path.join(os.getcwd(), "ontology_reports")

    # Redirect Python logging to Dagster context
    handler = logging.Handler()
    handler.emit = lambda record: context.log.info(record.getMessage())

    # Get logger from ontology-loader package
    controller_logger = logging.getLogger("ontology_loader.ontology_load_controller")
    controller_logger.setLevel(logging.INFO)
    controller_logger.addHandler(handler)

    context.log.info(f"Running Ontology Loader for ontology: {source_ontology}")
    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        output_directory=output_directory,
        generate_reports=generate_reports,
        mongo_client=context.resources.mongo.client,
        db_name=context.resources.mongo.db.name,
    )

    loader.run_ontology_loader()
    context.log.info(f"Ontology load for {source_ontology} completed successfully!")

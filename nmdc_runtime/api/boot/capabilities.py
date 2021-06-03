from nmdc_runtime.api.models.capability import Capability
import nmdc_runtime.api.boot.workflows as workflows_boot

# Include 1-to-1 "I can run this workflow" capabilities.
_raw = [item for item in workflows_boot._raw]


def construct():
    return [Capability(**kwargs) for kwargs in _raw]

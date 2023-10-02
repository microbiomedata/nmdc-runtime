import mock
from nmdc_runtime.site.changesheets.assets import (
    gold_biosamples_for_study,
    omics_processing_to_biosamples_map,
)
from nmdc_runtime.site.changesheets.resources import GoldApiResource, RuntimeApiUserResource

def test_gold_biosamples_for_study():
    assert True
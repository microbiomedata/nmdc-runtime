from nmdc_runtime.site.changesheets.gold_changesheet_generator import (
    BaseGoldBiosampleChangesheetGenerator, Issue397ChangesheetGenerator)


def test_base_gold_biosample_changesheet_generator(gold_biosample_response, gold_biosample_expected_names):
    changesheet_generator = BaseGoldBiosampleChangesheetGenerator("test_generator",
                                                                                      gold_biosample_response)
    assert changesheet_generator.gold_biosamples == gold_biosample_response

    gold_biosample_names = []
    for biosample in changesheet_generator.gold_biosamples:
        gold_biosample_names.append(changesheet_generator.get_biosample_name(biosample))
    assert gold_biosample_names == gold_biosample_expected_names


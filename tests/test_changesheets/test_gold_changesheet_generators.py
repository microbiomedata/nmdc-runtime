from nmdc_runtime.site.changesheets.gold_changesheet_generator import (
    BaseGoldBiosampleChangesheetGenerator,
    Issue397ChangesheetGenerator,
    get_gold_biosample_name_suffix,
    compare_biosamples,
    get_nmdc_biosample_object_id
)


def test_base_gold_biosample_changesheet_generator(gold_biosample_response, gold_biosample_expected_names):
    changesheet_generator = BaseGoldBiosampleChangesheetGenerator("test_generator",
                                                                  gold_biosample_response)
    assert changesheet_generator.gold_biosamples == gold_biosample_response

    gold_biosample_names = []
    for biosample in changesheet_generator.gold_biosamples:
        gold_biosample_names.append(get_gold_biosample_name_suffix(biosample))
    assert gold_biosample_names == gold_biosample_expected_names


def test_get_nmcd_biosample_object_id(nmdc_bona_009_biosample):
    assert get_nmdc_biosample_object_id(nmdc_bona_009_biosample) == "64e3e875a29dd0cc4d3cf756"


def test_compare_biosamples(nmdc_bona_009_biosample, gold_bona_009_biosample):
    line_items = compare_biosamples(nmdc_bona_009_biosample, gold_bona_009_biosample)
    assert len(line_items) == 0


def test_compare_biosamples_no_ecosystem_metadata(nmdc_bona_009_biosample_no_ecosystem_metadata,
                                                  gold_bona_009_biosample,
                                                  bona_009_no_ecosystem_metadata_expected_changesheet_line_items):
    line_items = compare_biosamples(nmdc_bona_009_biosample_no_ecosystem_metadata, gold_bona_009_biosample)
    assert line_items == bona_009_no_ecosystem_metadata_expected_changesheet_line_items


def test_compare_biosamples_no_gold_biosample_identifiers(
        nmdc_bona_009_biosample_no_gold_biosample_identifiers,
        gold_bona_009_biosample,
        bona_009_no_gold_biosample_identifiers_expected_changesheet_line_items):
    line_items = compare_biosamples(nmdc_bona_009_biosample_no_gold_biosample_identifiers, gold_bona_009_biosample)
    assert line_items == bona_009_no_gold_biosample_identifiers_expected_changesheet_line_items


def test_issue397_changesheet_generator(gold_biosample_response, omics_processing_to_biosamples_map, expected_omics_processing_to_biosamples_map):
    changesheet_generator = Issue397ChangesheetGenerator("test_generator",
                                                         gold_biosample_response,
                                                         omics_processing_to_biosamples_map)
    assert changesheet_generator.gold_biosamples == gold_biosample_response
    assert changesheet_generator.omics_processing_to_biosamples_map == expected_omics_processing_to_biosamples_map
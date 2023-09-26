from nmdc_runtime.site.changesheets.gold import (
    compare_biosamples,
)


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

from nmdc_schema import nmdc

from nmdc_runtime.site.translation.translator import Translator


class TestTranslator(Translator):
    def get_database(self) -> nmdc.Database:
        pass


def test_ensure_curie():
    assert (
        TestTranslator._ensure_curie("nmdc:bsm-11-z8x8p723", default_prefix="nmdc")
        == "nmdc:bsm-11-z8x8p723"
    )

    assert (
        TestTranslator._ensure_curie("bsm-11-z8x8p723", default_prefix="nmdc")
        == "nmdc:bsm-11-z8x8p723"
    )

    assert (
        TestTranslator._ensure_curie("gold:Gb0123456", default_prefix="nmdc")
        == "gold:Gb0123456"
    )

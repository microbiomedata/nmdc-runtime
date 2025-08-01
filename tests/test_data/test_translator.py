from nmdc_schema import nmdc

from nmdc_runtime.site.translation.translator import Translator


class TestTranslator(Translator):
    # Note: We define a `__test__` attribute whose value is `False`, as a way of telling
    #       pytest that this class is not a test class. If pytest thinks this class is a
    #       test class, pytest will complain that this class has an `__init__` method
    #       (which it does have, since it inherits one from the `Translator` class).
    #       Reference: https://docs.pytest.org/en/stable/example/pythoncollection.html#customizing-test-collection
    #
    __test__ = False

    # TODO: The type hint (`nmdc.Database`) is inconsistent with the return value (`None`).
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

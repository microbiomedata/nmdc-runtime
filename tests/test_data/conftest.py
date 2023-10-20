import string

import pytest
import random

from nmdc_runtime.minter.config import typecodes


@pytest.fixture
def test_minter():
    typecode_list = typecodes()

    def mint(type, how_many=1):
        typecode = next(t for t in typecode_list if t["schema_class"] == type)
        return [
            "nmdc:"
            + typecode["name"]
            + "-00-"
            + "".join(
                random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
            )
            for _ in range(how_many)
        ]

    return mint

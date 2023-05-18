import pytest
import uuid
import random


@pytest.fixture
def test_minter():
    def mint(type, how_many=1):
        return [
            "fake:"
            + str(
                uuid.UUID(
                    bytes=bytes(random.getrandbits(8) for _ in range(16)), version=4
                )
            )
            for _ in range(how_many)
        ]

    return mint

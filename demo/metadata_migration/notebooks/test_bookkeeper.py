import unittest
import re
from datetime import datetime, timedelta

from .bookkeeper import Bookkeeper


class TestBookkeeper(unittest.TestCase):
    r"""
    Tests targeting the `Bookkeeper` class.

    You can format this file like this:
    $ python -m black demo/metadata_migration/notebooks/test_bookkeeper.py

    You can run these tests like this:
    $ python -m unittest -v demo/metadata_migration/notebooks/test_bookkeeper.py

    Reference: https://docs.python.org/3/library/unittest.html#basic-example
    """

    def test_get_current_timestamp(self):
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$"
        ts_str = Bookkeeper.get_current_timestamp()

        # Verify the timestamp is a string having a valid format.
        self.assertIsInstance(ts_str, str)
        self.assertTrue(re.match(pattern, ts_str))

        # Verify the moment represented by the timestamp was within the past minute.
        ts = datetime.fromisoformat(ts_str)
        now_ts = datetime.now()
        time_difference = now_ts - ts
        self.assertLess(time_difference, timedelta(minutes=1))


if __name__ == "__main__":
    unittest.main()

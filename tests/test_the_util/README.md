This directory contains files related to testing code written in the `nmdc_runtime/util.py` file.

### Why this directory is not named `test_util`

I named the directory "`test_the_util`" to work around a limitation of pytest in the context of this repository.

I tried naming the directory "`test_util`" in an attempt to follow the naming convention of the other test directories.
In its name, "`test`" was a verb and "`util`" was a noun (i.e. "to test the utility"). This was in contrast to the file 
`../test_util.py` (a file that was already in the repository), in whose name "`test`" is an adjective and "`util`" is a
noun (i.e. "a test-related utility"). However, with those names in place, `pytest` reported this error:

```py
_____________________ ERROR collecting tests/test_util.py ______________________
import file mismatch:
imported module 'tests.test_util' has this __file__ attribute:
  /code/tests/test_util
which is not the same as the test file we want to collect:
  /code/tests/test_util.py
HINT: remove __pycache__ / .pyc files and/or use a unique basename for your test file modules
```

To work around that, I renamed the directory to `test_the_util` and renamed the contained Python file to match.

That is why the name of this test directory does not follow the naming convention
of the other test directories.

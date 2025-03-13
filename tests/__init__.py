import warnings

import dagster

# Ignore Dagster's beta warnings.
#
# Note: This line of code was originally preceeded by a comment saying only:
#       > "# XXX expecting to remove this on release of dagster 0.13.0"
#
# TODO: Consider removing this line of code in accordance with the comment
#       that originally preceeded it (which is shown above).
#
# Note: When we were using Dagster version `1.9.9`, we still had the `category`
#       kwarg set to `dagster.ExperimentalWarning`. When we updated Dagster
#       from `1.9.9` to `1.10.4`, we found that `dagter.ExperimentalWarning` no
#       longer existed. At that point, we changed it to `dagster.BetaWarning`
#       after seeing this GitHub Discussion comment:
#       https://github.com/dagster-io/dagster/discussions/13112#discussioncomment-12419858
#
# Reference: https://docs.python.org/3/library/warnings.html#warnings.filterwarnings
#
warnings.filterwarnings("ignore", category=dagster.BetaWarning)

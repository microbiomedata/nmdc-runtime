When this file is changed and pushed to the `main` branch on GitHub, the deployment on NERSC Spin is
refreshed ([via a GitHub
action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/release-to-spin.yml))
to use the latest Docker images (which may be rebuilt [via this GitHub
Action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/build-and-push-docker-images.yml)).

Add a new line to the log below, in reverse chronological order (so the latest release at top), when
you wish to trigger a release. Ensure that the latest Docker images have been built and pushed prior
to triggering a release.

Use <https://en.wikipedia.org/wiki/ISO_8601> to express the current date and time. "−05:00" is the
time offset for New York on standard time (EST). "−08:00" would be for California.

## Release Log

* 2021-11-10T16:01:00−05:00 Fixed cmd in github action.
* 2021-11-10T15:58:00−05:00 Testing github action.




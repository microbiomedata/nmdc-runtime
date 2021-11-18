When this file is changed and pushed to the `main` branch on GitHub,

1. the Docker images for the API and Runtime are rebuild and pushed [via this GitHub
   Action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/build-and-push-docker-images.yml)), and

2. if the previous action is successful, the deployment on NERSC Spin is refreshed ([via a GitHub
   action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/release-to-spin.yml))

Add a new line to the log below, in reverse chronological order (so the latest release at top), when
you wish to trigger a Docker-images rebuild and a subsequent release to Spin.

Use <https://en.wikipedia.org/wiki/ISO_8601> to express the current date and time. "−05:00" is the
time offset for New York on standard time (EST). "−08:00" would be for California.

## Release Log

* 2021-11-18T11:44:00−05:00 add `count` command option to /queries
* 2021-11-17T08:05:00−05:00 Ensure `"disabled": false` is the default for new users.
* 2021-11-15T12:25:00−05:00 Add nmdc schema collection endpoints; allow creating users
* 2021-11-10T16:39:00−05:00 Fixed usernames check in api logic
* 2021-11-10T16:01:00−05:00 Fixed cmd in github action.
* 2021-11-10T15:58:00−05:00 Testing github action.




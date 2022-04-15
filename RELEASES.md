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

* 2022-04-14T21:38:00−04:00 test re-deploy with refreshed credentials
* 2022-04-14T13:05:00−04:00 add sort to search api. fix cursor-based pagination.
* 2022-04-08T14:15:00−04:00 init basic search api
* 2022-04-07T17:30:00−04:00 update docker image bases
* 2022-03-23T17:15:00−04:00 idempotent api startup
* 2022-03-23T15:04:00−04:00 fix below...again
* 2022-03-23T14:58:00−04:00 fix below
* 2022-03-23T14:50:00−04:00 fix dagster JSON serialization error 
* 2022-03-18T12:33:00−04:00 upgrade to nmdc-schema==3.2.0
* 2022-03-08T15:00:00−05:00 add get_by_id endpoint in /nmdcschema
* 2022-01-20T20:17:00−08:00 Bump schema back to 2022.01.18rc1
* 2022-01-20T20:13:00−08:00 Tweak module versions
* 2022-01-20T17:45:00−08:00 Pin python module versions
* 2022-01-19T09:45:00−05:00 update nmdc-schema to version 2022.01.18rc1
* 2022-01-18T21:08:33−08:00 removed monkey patch of schema based on mongo.db.file_type_enum
* 2021-11-18T14:49:00−05:00 fix metadata-in process for data_object_set payloads
* 2021-11-18T11:44:00−05:00 add `count` command option to /queries
* 2021-11-17T08:05:00−05:00 Ensure `"disabled": false` is the default for new users.
* 2021-11-15T12:25:00−05:00 Add nmdc schema collection endpoints; allow creating users
* 2021-11-10T16:39:00−05:00 Fixed usernames check in api logic
* 2021-11-10T16:01:00−05:00 Fixed cmd in github action.
* 2021-11-10T15:58:00−05:00 Testing github action.




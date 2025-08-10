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
* 2023-11-07T19:45:00-08:00 update nmdc-schema package from 9.0.4 to 9.1.0
* 2023-11-07T19:30:00-08:00 update nmdc-schema package from 8.1.2 to 9.0.4
* 2023-11-07T17:30:00-08:00 update nmdc-schema package from 8.0.0 to 8.1.2
* (missing entries)
* 2023-08-31T22:15:00-07:00 update nmdc-schema package from 7.7.2 to 7.8.0
* 2023-01-27T13:13:09-05:00 return 201 on activity creation
* 2023-01-25T13:13:09-05:00 all typecodes for minter
* 2023-01-23T10:31:32-05:00 fix Entity type in case os.getenv() returns `None`
* 2023-01-23T10:31:32-05:00 fix Entity type in case os.getenv() returns `None`
* 2023-01-23T10:31:32-05:00 fix Entity type
* 2023-01-23T10:31:32-05:00 update workflow spec
* 2023-01-23T10:31:32-05:00 re-add id minter
* 2023-01-21T10:31:32-05:00 remove id minter
* 2023-01-21T10:31:32-05:00 use dict for sequencing activity
* 2023-01-21T10:31:32-05:00 fix activities
* 2023-01-20T10:31:32-05:00 enable minter again
* 2023-01-19T10:31:32-05:00 add more endpoints
* 2023-01-19T10:31:32-05:00 remove minter for now
* 2023-01-19T10:31:32-05:00 /pids/* to mint and manage draft IDs
* 2023-01-16T12:27:05-05:00 upgrade to python 3.10
* 2022-01-10T11:05:00-04:00 reenable site auth
* 2022-01-10T11:05:00-04:00 propagate jobs
* 2022-01-10T11:05:00-04:00 dump json
* 2022-01-10T11:05:00-04:00 change model for outputs
* 2022-01-09T11:05:00-04:00 update remove init beanie
* 2022-01-09T11:05:00-04:00 update pinned dependencies
* 2022-01-09T11:05:00-04:00 revert outputs
* 2022-12-18T11:05:00-04:00 revert outputs
* 2022-11-30T11:05:00-04:00 return op.dict in util
* 2022-11-22T11:05:00-04:00 revert returning full drs object in job config
* 2022-11-14T11:05:00-04:00 return full drs object in job config
* 2022-10-27T11:05:00-04:00 return workflow after claiming job
* 2022-09-29T11:05:00-04:00 remove second call to mongo
* 2022-09-28T16:37:00-04:00 use env-supplied db name, etc.
* 2022-09-28T16:42:00-04:00 update outputs endpoint
* 2022-09-28T15:42:00-04:00 remove user
* 2022-09-28T15:42:00-04:00 return serialized object
* 2022-09-27T15:42:00-04:00 return drs object
* 2022-09-27T14:42:00-04:00 readsqc-in trigger
* 2022-09-27T16:05:00-04:00 fix api endpoint for outputs
* 2022-09-11T16:04:00-04:00 fix api endpoint for outputs
* 2022-09-11T16:03:00-04:00 add workflow and infrastructure components
* 2022-08-11T16:03:00-04:00 upgrade to dagster 1.0.x
* 2022-08-04T12:56:00-04:00 api perms to db; back up docs on /queries:run(query_cmd:DeleteCommand)
* 2022-08-03T12:06:00-04:00 change perms
* 2022-07-12T12:28:00-04:00 fix: multi-collection and float-value-range-containing changesheets
* 2022-06-16T16:14:00-04:00 /runs soft release; perms mod
* 2022-04-21T15:57:00-04:00 draft /runs and /run-events API resources
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

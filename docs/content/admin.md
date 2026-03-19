# Administration

This document is about administering the production instance of the NMDC Runtime, which is hosted
at [api.microbiomedata.org](https://api.microbiomedata.org).

## Hosting

We currently host the NMDC Runtime on Google Cloud Platform. More specifically, we host it on GKE
(Google Kubernetes Engine). The underlying MongoDB server, as well as Dagster and its Postgres
server, are also hosted on GKE.

We use Cloudflare to proxy requests from the Internet to GKE.

## Health checks

### Health check API endpoint

The Runtime has a dedicated health check API endpoint, at `GET /health`. The endpoint's response
payload looks like this:

```json
{
  "web_server": true,
  "database": true
}
```

When the Runtime receives a request at that endpoint, it checks two things:

1. It checks whether the FastAPI application can receive and respond to HTTP requests. This will
   be `true` in every HTTP response from this endpoint.
2. It checks whether the FastAPI application can read from the MongoDB server. This will be either
   `true` or `false`.

When either of the above checks fail, the endpoint returns a status code of
`HTTP 503 Service Unavailable`. Otherwise, the endpoint returns a status code of `HTTP 200 OK`.
In either situation, the HTTP response payload will include a breakdown of the checks and their
results.

### Cloudflare health checks

We have configured Cloudflare to perform health checks of the production NMDC Runtime API and,
whenever the health status changes, to send a message to the NMDC Slack workspace
(i.e. the `#cloudflare-notifications` channel).

## Create API Users

Users that are admins of the `nmdc-runtime-useradmin` site may create API users.

Any Runtime user can get a list of the sites that they, themselves, administer. They can do that
by logging into the Runtime API and sending a request to `GET /users/me`. The response will look
like this:

```json
{
   "username": "bob",
   "site_admin": [
      "nersc",
      "bobs-laptop"
   ]
}
```

If you are an admin (as defined above), you can create a user by submitting an HTTP request to the
`POST /users` endpoint. When creating a new user, we recommend sending them their password via
a secret sharing service such as [SnapPass](https://github.com/pinterest/snappass). If the sender
and recipient are LBNL employees, the sender can use [LBNL's own SnapPass instance](https://snappass.lbl.gov).

## Managing API permissions

The Runtime was designed to use an "allowance" system. An "allowance" is a MongoDB document that maps
a user to an action. There are various checks throughout the codebase for whether the current user
is allowed to perform a given action—those checks use those "allowance" documents.

In the past, Runtime admins would manage "allowances" via direct MongoDB commands. However, we have
since introduced some admin-only API endpoints that admins can use to manage "allowances" via the
Runtime API. While there is no dedicated Runtime admin UI, admins can use Swagger UI for this.

## MongoDB

The MongoDB instance underlying the NMDC Runtime is hosted on Google Cloud (specifically, on GKE).
Team members wanting to become familiar with that instance can learn about it by perusing the
private `microbiomedata/infra-admin` GitHub repository.

For example, the NMDC team uses custom MongoDB user roles; and the `mongosh` script that can be used
to create those roles is stored in that GitHub repository—not in the `nmdc-runtime` repository.

### MongoDB databases

Data is stored in the `nmdc` database.

Whenever you delete a document, copy it to the corresponding collection in the `nmdc_deleted`
database. Documents in the latter collection act as breadcrumbs whose existence facilitates
subsequent investigation.

## Bumping the `nmdc-schema` dependency

Here's how you can update the `nmdc-schema` package upon which the Runtime depends.

1. Update the `dependencies` list in `pyproject.toml` so it references the new version of
   `nmdc-schema`; for example:

   ```diff
   - "nmdc-schema == 11.16.1",
   + "nmdc-schema == 11.17.0",
   ```

2. Synchronize the transitive dependencies by running:

   ```shell
   docker compose run --rm --no-deps fastapi sh -c 'uv sync --active'
   ```

3. Run the tests and confirm they all pass.

   ```shell
   make test
   ```

   > This step is necessary because schema changes can introduce new constraints on the data
   > processed by the Runtime, and some of the Runtime's tests use example data that may not
   > meet those constraints. If any tests fail, determine the root cause of the failure,
   > address the root cause, and re-run the tests.

4. Commit the changes to the repository.

   ```sh
   git add pyproject.toml uv.lock
   git commit -m 'Bump `nmdc-schema` version'
   git push
   ```

   > Note: You can customize the commit message to indicate the _specific version_ to which you
   > updated the `nmdc-schema` package (e.g. "Bump `nmdc-schema` version to `11.17.0`").

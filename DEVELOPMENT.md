# Development

The NMDC Runtime is built upon the following technologies:

- [Python 3.12](https://www.python.org/downloads/release/python-3120/) (programming language)
- [FastAPI](https://fastapi.tiangolo.com/) (API framework)
- [MongoDB](https://www.mongodb.com/) (NoSQL database)
- [Dagster](https://dagster.io/) + [PostgreSQL](https://www.postgresql.org/) (data orchestrator)
- [LinkML](https://linkml.io/) + [NMDC Schema](https://w3id.org/nmdc/nmdc) (data validator)

## Local development

The NMDC Runtime has an official development environment, which is container based.
Here's how you can spin it up locally.

### Prerequisites

- You are using macOS
  - Other operating systems may work as alternatives (we do not routinely test them)
- [Make](https://www.gnu.org/software/make/) is installed
  - [How to install Make on macOS](https://formulae.brew.sh/formula/make)
- (Recommended) [uv](https://docs.astral.sh/uv/) is installed
  - While not technically necessary for basic functionality (although some optional `make` targets _do_ require it), we recommend that you have `uv` installed locally. That way, you can use it to install the Python dependencies locally, which can enable features in your code editor (such as auto-completion and type checking).
- [Docker](https://www.docker.com/products/docker-desktop/) is running
  - Other container runtimes—such as [podman](https://podman.io/) and [colima](https://colima.run/)—may work as alternatives (we do not routinely test them)

### Quick start

1. Clone this repository and enter the clone's root directory.

   ```sh
   git clone https://github.com/microbiomedata/nmdc-runtime
   cd nmdc-runtime
   ```

2. Limit access to the included Mongo [KeyFile](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-with-keyfile-access-control/#keyfile-security).

   ```sh
   chmod 600 .docker/mongoKeyFile
   ```

3. Configure the environment.

   ```sh
   cp .env.example .env
   vi .env  # use any text editor
   ```

   > Refer to the comments within the `.env.example` file for guidance.

4. Spin up the development environment.

   ```sh
   make up-dev
   ```

5. Use the development environment.

   1. API base URL: `http://127.0.0.1:8000`
   2. Swagger UI: [`http://127.0.0.1:8000/docs`](http://127.0.0.1:8000/docs)
   3. Dagster UI: [`http://127.0.0.1:3000`](http://127.0.0.1:3000)
   4. MongoDB [connection string](https://www.mongodb.com/docs/v8.0/reference/connection-string-options/#connection-string-options): `mongodb://admin:root@127.0.0.1:27018/nmdc?authSource=admin`

   > Note: The port numbers above may differ in your environment, depending upon the contents of your `.env` file.

### Coding

#### Dependency management

We use [`uv`](https://docs.astral.sh/uv/) to manage dependencies of the application. Here's how you can use `uv` both on your host machine and within a container in the Docker Compose stack.

<!-- ---------------------------------------------------------------------- -->

<details>
<summary>1. On the host</summary>

---

Although we typically run the application within a container, some developers prefer that application's dependencies be installed locally also (so that their code editors will provide auto-completion, type checking, etc.).

Here's how you can install the application's dependencies locally:

```sh
uv sync
```

That will...

1. (If you made changes to `pyproject.toml`) **Update the lock file** (at `uv.lock`) to reflect those changes
2. (If a Python virtual environment doesn't exist at `.venv/` yet) **Create a Python virtual environment** at `.venv/`
3. (If the Python virtual environment and `uv.lock` files are out of sync) **Synchronize the Python virtual environment** with `uv.lock` (by installing and uninstalling packages)

---

</details>

<!-- ---------------------------------------------------------------------- -->

<details>
<summary>2. Within a container</summary>

---

In the Docker Compose stack, the Python virtual environment is located at the path specified by the `VIRTUAL_ENV` environment variable (which is defined in the `Dockerfile`) instead of at `.venv/`. That helps with containerization, but it deviates from `uv`'s default behavior, which is to use the Python virtual environment at `.venv/`. So, when running `uv` commands within the Docker Compose stack, we always include the [`--active`](https://docs.astral.sh/uv/reference/cli/#uv-sync--active) flag (which tells `uv` to use the Python virtual environment at the path specified by `VIRTUAL_ENV`).

Here's how you can install the application's dependencies within a container in the Docker Compose stack:

```sh
uv sync --active
```

---

</details>

<!-- ---------------------------------------------------------------------- -->

#### Formatting

We use [Black](https://black.readthedocs.io/en/stable/) to **format** Python source code files.
Once you have the application's dependencies installed, you can run Black via:

```sh
make black
```

#### Linting

We use [Flake8](https://flake8.pycqa.org/en/latest/) to **lint** Python source code files.
Once you have the application's dependencies installed, you can run Flake8 via:

```sh
make lint
```

### Testing

The official development environment includes a test stack (which is described in `docker-compose.test.yml`) that is distinct from the development stack (which is described in `docker-compose.yml`). Here's how you can use the test stack:

- Run all tests.

  ```sh
  make test
  ```

- Run all tests in a specific file.

  ```sh
  make test ARGS="tests/test_api/test_endpoints.py"
  ```

- **Spin up the test stack and launch a shell** within the test runner container, but don't run any tests automatically.

  ```sh
  make test-shell
  ```

- **Delete the Mongo data** in the test stack (useful whenever a failing test does not clean up after itself; as some older tests do not use self-cleaning fixtures or `finally` blocks).

  ```sh
  make clear-db-test
  ```

- **Reset the FastAPI container** in the test stack (useful for test-driven development, since—unlike in the development stack—the [Uvicorn web server](https://uvicorn.dev/settings/#development) in the _test_ stack does not run in "watch" mode).

  ```sh
  make reset-fastapi-test
  ```

## Appendix

<!-- ---------------------------------------------------------------------- -->

<details>
<summary>1. Customizing the base site's site client credentials</summary>

---

> Note: For most developers, this is unnecessary.

Some environment variables in the `.env` file are only processed during the _first boot_ of the FastAPI container. That includes the `API_SITE_CLIENT_ID` and `API_SITE_CLIENT_SECRET` environment variables, which dictate the credentials (i.e. `client_id` and `client_secret`) of the base site's site client.

To people who want the base site's site client credentials to have values other than the default ones (e.g. in order to match some values in a dependent application—which is not common), we recommend customizing those environment variables **before** starting up the FastAPI container.

In case you have **already** started up the FastAPI container (this is common), all is not lost! You can still customize the credentials. Here's how:

1. Stop the containers that depend upon Mongo.

   ```sh
   docker compose stop fastapi dagster-daemon dagster-dagit
   ```

2. Launch `mongosh` within the `mongo` container.

   ```sh
   docker compose exec mongo mongosh -- "mongodb://admin:root@127.0.0.1:27017/nmdc?authSource=admin"
   ```

3. At the `mongosh` shell, delete the base site from the `sites` collection.

   > Replace `generateme` in the command below, with the ID of the base site, which you can get from the `API_SITE_ID` environment variable in your `.env` file.

   ```js
   db.getSiblingDB("nmdc").getCollection("sites").deleteOne({ id: "generateme" });
   ```

4. Update the `API_SITE_CLIENT_ID` and `API_SITE_CLIENT_SECRET` environment variables in your `.env` file, so they contain the values that you want as your base site's site client credentials (i.e. `client_id` and `client_secret`).
5. Restart the containers you stopped earlier.

   ```sh
   docker compose start fastapi dagster-daemon dagster-dagit
   ```

---

</details>

<!-- ---------------------------------------------------------------------- -->

<details>
<summary>2. Testing Dagster assets and ops</summary>

---

From [Unit testing assets and ops](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops):

> Unit testing is essential for ensuring that computations function as intended. In the context of data pipelines, this can be particularly challenging. However, Dagster streamlines the process by enabling direct invocation of computations with specified input values and mocked resources, making it easier to verify that data transformations behave as expected.
>
> While unit tests can't fully replace integration tests or manual review, they can catch a variety of errors with a significantly faster feedback loop.

Visit the page linked above to learn about testing Dagster assets and ops.

---

</details>

<!-- ---------------------------------------------------------------------- -->

<details>
<summary>3. Performance profiling</summary>

---

We use a tool called [Pyinstrument](https://pyinstrument.readthedocs.io) to profile the performance of the Runtime API while processing an individual HTTP request.

Here's how you can do that:

1. In your `.env` file, set `IS_PROFILING_ENABLED` to `true`
2. Start/restart your development stack: `$ make up-dev`
3. Ensure the endpoint function whose performance you want to profile is defined using `async def` (as opposed to just `def`) ([reference](https://github.com/joerick/pyinstrument/issues/257))

Then—with all of that done—submit an HTTP request that includes the URL query parameter: `profile=true`. Instructions for doing that are in the sections below.

<details>
<summary>Show/hide instructions for <code>GET</code> requests only (involves web browser)</summary>

1. In your web browser, visit the endpoint's URL, but add the `profile=true` query parameter to the URL. Examples:

   ```diff
   A. If the URL doesn't already have query parameters, append `?profile=true`.
   - http://127.0.0.1:8000/nmdcschema/biosample_set
   + http://127.0.0.1:8000/nmdcschema/biosample_set?profile=true

   B. If the URL already has query parameters, append `&profile=true`.
   - http://127.0.0.1:8000/nmdcschema/biosample_set?filter={}
   + http://127.0.0.1:8000/nmdcschema/biosample_set?filter={}&profile=true
   ```

2. Your web browser will display a performance profiling report.
   > Note: The Runtime API will have responded with a performance profiling report web page, instead of its normal response (which the Runtime discards).

That'll only work for `GET` requests, though, since you're limited to specifying the request via the address bar.

</details>

<details>
<summary>Show/hide instructions for <strong>all</strong> kinds of requests (involves <code>curl</code> + web browser)</summary>

1. At your terminal, type or paste the `curl` command you want to run (you can copy/paste one from Swagger UI).
2. Append the `profile=true` query parameter to the URL in the command, and use the `-o` option to save the response to a file whose name ends with `.html`. For example:

   ```diff
     curl -X 'POST' \
   -   'http://127.0.0.1:8000/metadata/json:validate' \
   +   'http://127.0.0.1:8000/metadata/json:validate?profile=true' \
   +    -o /tmp/profile.html
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{"biosample_set": []}'
   ```

3. Run the command.
   > Note: The Runtime API will respond with a performance profiling report web page, instead of its normal response (which the Runtime discards). The performance profiling report web page will be saved to the `.html` file to which you redirected the command output.
4. Double-click on the `.html` file to view it in your web browser.
   1. Alternatively, open your web browser and navigate to the `.html` file; e.g., enter `file:///tmp/profile.html` into the address bar.

</details>

---

</details>

<!-- ---------------------------------------------------------------------- -->

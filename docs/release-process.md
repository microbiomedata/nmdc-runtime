# Release Process

How do new versions of the API and NMDC Runtime site (Dagster daemon and Dagit frontend) get
released? Here's how.

1. Ensure the tests pass (i.e., a "smoke test").
    
    - Either run tests locally
    ```
    make up-test
    make test
    ```
   - or confirm the test pass via 
     [our python-app.yml GitHub
     action](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/python-app.yml),
     which is triggered automatically when a change to any Python file in the repository is pushed to the
     `main` branch, or to a Pull Request. You can monitor the status of Github Actions
     [here](https://github.com/microbiomedata/nmdc-runtime/actions).

2. Add a summary for the release to `RELEASES.md`. You can make an edit and push to the `main`
   branch [via GitHub](https://github.com/microbiomedata/nmdc-runtime/blob/main/RELEASES.md). This will
   trigger two GitHub actions in sequence to

       - [build and push updated Docker images](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/build-and-push-docker-images.yml) for the API server and for the NMDC Runtime site's Dagster daemon and Dagit dashboard, and

       - [deploy the new images to Spin](https://github.com/microbiomedata/nmdc-runtime/blob/main/.github/workflows/release-to-spin.yml).
# Administration

## Create API Users

Users that are admins of the `nmdc-runtime-useradmin` site may create API users. Currently, these
users are `scanon`, `dehays`, and `dwinston`.

You can see what sites you administer via `GET /users/me` when logged in.

!!! example "example `GET /users/me` result"
    ```json
    {
      "username": "dwinston",
      "site_admin": [
        "dwinston-laptop",
        "nmdc-runtime-useradmin"
      ]
    }
    ```

Log in via your username and password, and `POST /users` to create a new user. The only required
fields are `username` and `password`.

## MongoDB Administration

The MongoDB instance backing the runtime is deployed on NERSC Spin.

The root admin password is stored as the `mongo-root-password` secret in the `nmdc-runtime-dev`
namespace on the Spin k8s development cluster
([link](https://rancher2.spin.nersc.gov/p/c-fwj56:p-nlxq2/secrets/nmdc-runtime-dev:mongo-root-password)).

`scanon` and `dehays` have `dbOwner` roles on the `nmdc` database.

!!! tip
    There is a `nersc-ssh-tunnel` target in the repository's
    [`Makefile`](https://github.com/microbiomedata/nmdc-runtime/blob/main/Makefile)
    that can help you map the remote mongo database to a port on your local machine.

## Deployment

The [release process](release-process.md) is administered by the [NMDC architecture working group
GitHub team](https://github.com/orgs/microbiomedata/teams/architecture-wg). Members of this team
have full access to repository administration, including the GitHub Actions.

As for the deployed infrastructure, when manual intervention may be necessary, first check the
Rancher 2 web interface to the NERSC Spin service's Kubernetes clusters, i.e.
<https://rancher2.spin.nersc.gov/>. The Runtime system is currently deployed on the development
cluster as part of the NMDC's `m3408` project, under the `nmdc-runtime-dev` namespace.

The go-to people to troubleshoot deployment issues within NERSC Spin at this time are `dehays`,
`dwinston`, and `scanon`.

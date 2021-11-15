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

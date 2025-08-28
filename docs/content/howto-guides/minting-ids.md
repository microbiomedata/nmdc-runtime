# Minting IDs

## Overview

In this how-to guide, I'll show you how you can mint an ID using the NMDC Runtime API.

At a high level, the procedure is:
1. Obtain site client credentials
2. Log in as a site client
3. Request that an ID be minted

## Obtain site client credentials

The API endpoint that mints IDs is only accessible to Runtime **site clients**—it is not accessible to regular Runtime users.
Accessing the Runtime as a site client involves using **site client credentials**.

Here's how you can obtain site client credentials:

1. Visit the NMDC Runtime API's [Swagger UI](https://api.microbiomedata.org/docs). 
2. Login as a regular Runtime user.
   1. Near the upper right corner of the page, click the "Authorize" button.
   2. In the topmost section—entitled "User login"—enter your Runtime username and password, then click the "Authorize" button.
   3. Click the "Close" button to close the "Available authorizations" popup.
3. Check whether your user account is already an admin of any sites.
   1. Expand the `GET /users/me` API endpoint section.
   2. Click the "Try it out" button on the right.
   3. Click the "Execute" button.
   4. Check whether the "Response body" value has any items in its `site_admin` list.
      1. If it does, your user account is already an admin of a site. In this case, **skip top-level step 4**, which is about creating a site—and go straight to top-level step 5, which is about generating site client credentials.
      2. If it doesn't, your user account is not already an admin of a site. In this case, **continue to top-level step 4 to create a site**.
4. **Create a site**.
   1. Expand the `POST /sites` API endpoint section.
   2. Click the "Try it out" button on the right.
   3. In the "Request body" section, replace `"id": "string"` with `"id": "username_site"` (replace `username` with your Runtime username). For example: `"id": "johnny_site"`.
   4. Click the "Execute" button.
   5. Check whether the "Code" next to the "Response body" says `201`.
      1. If it does, **your user account is now the admin of a site**.
      2. If it doesn't, try a different `id` value or ask a colleague for help.
5. Generate site client credentials.
   1. Expand the `POST /sites/{site_id}:generateCredentials` API endpoint section.
   2. Click the "Try it out" button on the right.
   3. In the `site_id` box, enter the `id` value of a site of which your user account is an admin. For example: `johnny_site`.
   4. In the "Response body" section, copy the site client credentials—i.e., the `client_id` value and the `client_secret` value—into a secure location (e.g., a [password manager](https://bitwarden.com/)) for future reference.

At this point, you have obtained site client credentials. In the next section, you'll use them to log into the NMDC Runtime API as a site client.

## Log in as a site client

Here's how you can log into the NMDC Runtime API as a site client:

1. Visit the NMDC Runtime API's [Swagger UI](https://api.microbiomedata.org/docs) (if you aren't already there).
2. Log in as a site client.
   1. Near the upper right corner of the page, click the "Authorize" button.
   2. Scroll down to the section entitled "Site client login".
   3. In that section, enter your site client credentials—i.e., your `client_id` and `client_secret`—then click the "Authorize" button.
   4. Click the "Close" button to close the "Available authorizations" popup.

At this point, you're logged into the NMDC Runtime API as a site client. In the next section, you'll request that an ID be minted.

## Request that an ID be minted

Requesting that an ID be minted—typically referred to as "minting an ID"—involves submitting a request to a specific API endpoint.
The request includes two pieces of information:
- the number of IDs you want to mint
- the `class_uri` value of the [schema class](https://microbiomedata.github.io/nmdc-schema/#classes) for which you want to mint IDs
  (for example, the `class_uri` value of the `Study` class is `nmdc:Study`).

Here's how you can mint an ID:

1. Visit the NMDC Runtime API's [Swagger UI](https://api.microbiomedata.org/docs) (if you aren't already there).
2. Log in as a site client (if you aren't already logged in as one).
3. Mint an ID
   1. Expand the `POST /pids/mint` API endpoint section.
   2. Click the "Try it out" button on the right.
   3. In the "Request body" section, within the `"schema_class": { ... }` object, replace "`nmdc:Biosample`" with the `class_uri` of the [schema class](https://microbiomedata.github.io/nmdc-schema/#classes) you want to mint an ID for. For example (to mint IDs for two studies):
      ```json
      {
        "schema_class": {
          "id": "nmdc:Study"
        },
        "how_many": 2
      }
      ```
   4. Click the "Execute" button.
   5. The "Response body" section will contain the newly-minted IDs. For example:
      ```json
      [
        "nmdc:sty-11-m4lak1ng",
        "nmdc:sty-11-puw1tm09"
      ]
      ```

At this point, you have used the NMDC Runtime API to mint IDs.

## Conclusion

In this how-to guide, I showed you how you could obtain site client credentials, use those site client credentials to log into the NMDC Runtime API as a site client, and request that the NMDC Runtime mint some IDs.

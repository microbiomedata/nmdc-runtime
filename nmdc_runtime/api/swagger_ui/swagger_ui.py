"""Constants related to configuring Swagger UI."""

# Reference: https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/#parameters
base_swagger_ui_parameters: dict = {
    "withCredentials": True,
    # Collapse the "Schemas" section by default.
    # Note: `-1` would omit the section entirely.
    "defaultModelsExpandDepth": 0,
    # Display the response times of the requests performed via "Try it out".
    # Note: In my local testing, the response times reported by this
    #       are about 50-100ms longer than the response times reported
    #       by Chrome DevTools. That is the case whether the actual
    #       response time is short (e.g. 100ms) or long (e.g. 60s);
    #       i.e. not proportional to the actual response time.
    "displayRequestDuration": True,
    # Make all sections (i.e. groups of endpoints) _collapsed_ by default.
    # Note: `"list"` would make all sections _expanded_ by default.
    "docExpansion": "none",
    # Make it so a logged-in user remains logged in even after reloading
    # the web page (or leaving the web page and coming back to it later).
    "persistAuthorization": True,
}

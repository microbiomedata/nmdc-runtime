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
    # Expand all sections (i.e. groups of endpoints) by default.
    "docExpansion": "list",
    # Make it so a logged-in user remains logged in even after reloading
    # the web page (or leaving the web page and coming back to it later).
    "persistAuthorization": True,
    # Specify the Swagger UI plugins we want to use (see note below).
    #
    # Note: FastAPI's `get_swagger_ui_html` function always serializes
    #       the value of this property as a _string_, while the Swagger UI
    #       JavaScript code requires it to be an _array_. To work around that,
    #       we just add a placeholder string here; then, after we pass this
    #       dictionary to FastAPI's `get_swagger_ui_html` function and get the
    #       returned HTML for the web page, we replace this placeholder string
    #       (within the returned HTML) with the JavaScript array we wanted
    #       the "plugins" property to contain all along.
    #
    "plugins": r"{{ NMDC_SWAGGER_UI_PARAMETERS_PLUGINS_PLACEHOLDER }}",
}

console.debug("Listening for event: nmdcInit");
window.addEventListener("nmdcInit", (event) => {{
    console.debug("Detected event: nmdcInit");

    // Get the DOM elements we'll be referencing below.
    const bodyEl = document.querySelector("body");

    // If the access token is present in the DOM (see `main.py`), create and add a banner
    // displaying the token along with buttons to show/hide it and copy it to the clipboard.
    const accessToken = document.getElementById("nmdc-access-token")?.getAttribute("data-token");
    if (typeof accessToken === "string") {
        console.debug("Adding token banner");

        // Create the banner.
        const sectionEl = document.createElement("section");
        sectionEl.classList.add("nmdc-info", "nmdc-info-token", "block", "col-12");
        sectionEl.innerHTML = `
            <p>You are now authorized. Prefer a command-line interface (CLI)? Use this header for HTTP requests:</p>
            <p>
                <code>
                    <span>Authorization: Bearer </span>
                    <span id="token" data-token-value="${accessToken}" data-state="masked">***</span>
                </code>
            </p>
            <p>
                <button id="token-mask-toggler">Show token</button>
                <button id="token-copier">Copy token</button>
                <span id="token-copier-message"></span>
            </p>
        `;

        // Mount the banner to the DOM.
        document.querySelector(".information-container").append(sectionEl);

        // Get references to DOM elements within the banner that was mounted to the DOM.
        const tokenMaskTogglerEl = document.getElementById("token-mask-toggler");
        const tokenEl = document.getElementById("token");
        const tokenCopierEl = document.getElementById("token-copier");
        const tokenCopierMessageEl = document.getElementById("token-copier-message");

        // Set up the token visibility toggler.
        console.debug("Setting up token visibility toggler");
        tokenMaskTogglerEl.addEventListener("click", (event) => {{
            if (tokenEl.dataset.state == "masked") {{
                console.debug("Unmasking token");
                tokenEl.dataset.state = "unmasked";
                tokenEl.textContent = tokenEl.dataset.tokenValue;
                event.target.textContent = "Hide token";
            }} else {{
                console.debug("Masking token");
                tokenEl.dataset.state = "masked";
                tokenEl.textContent = "***";
                event.target.textContent = "Show token";
            }}
        }});

        // Set up the token copier.
        // Reference: https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
        console.debug("Setting up token copier");
        tokenCopierEl.addEventListener("click", async (event) => {{
            tokenCopierMessageEl.textContent = "";
            try {{                            
                await navigator.clipboard.writeText(tokenEl.dataset.tokenValue);
                tokenCopierMessageEl.innerHTML = "<span class='nmdc-success'>Copied to clipboard</span>";
            }} catch (error) {{
                console.error(error.message);
                tokenCopierMessageEl.innerHTML = "<span class='nmdc-error'>Copying failed</span>";
            }}
        }});
    }

    /**
     * Customizes the login form.
     * 
     * Prerequisite: The login form must be present in the DOM.
     */
    const customizeLoginForm = () => {
        const modalContentEl = document.querySelector('.auth-wrapper .modal-ux-content');

        console.debug("Customizing login form headers");
        const formHeaderEls = modalContentEl.querySelectorAll('.auth-container h4');
        formHeaderEls.forEach(el => {
            // Update the header text based on its current value.
            switch (el.textContent.trim()) {
                case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, password)":
                    el.textContent = "User login";
                    break;
                case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, clientCredentials)":
                    el.textContent = "Site client login";
                    break;
                // Note: This default string has a `U+00a0` character before the space.
                case "bearerAuth  (http, Bearer)":
                    break; // do nothing
                default:
                    console.debug(`Unrecognized header: ${el.textContent}`);
            }
        });

        console.debug("Focusing on username field if present");
        const usernameInputEl = modalContentEl.querySelector("input#oauth_username");
        if (usernameInputEl !== null) {
            usernameInputEl.focus();
        }
    };
    console.debug("Setting up event listener for customizing login form");
    //
    // Listen for a "click" event on the `body` element, check whether the element that was clicked
    // was the "Authorize" button (or one of its descendants), and if so, customize the modal login
    // form that will have been mounted to the DOM by the time the "click" event propagated to the
    // `body` element and our event handler was called.
    //
    // Note: We attach this event listener to the `body` element because that's the lowest-level
    //       element where we found that mounting it doesn't cause our event handler to run too early
    //       (i.e. doesn't cause it to run _before_ the event handlers that mount the modal login form
    //       to the DOM have run). Our event handler needs that form to be mounted so it can access
    //       its elements.
    //       
    //       If we were to attach it to a lower-level element (e.g. directly to the "Authorize" button),
    //       we would have to, for example, make its body a `setTimeout(fn, 0)` callback in order to
    //       defer its execution until all the event handlers for the "click" even have run.
    //
    bodyEl.addEventListener("click", (event) => {
        // Check whether the clicked element was the "Authorize" button or any of its descendants.
        if (event.target.closest(".auth-wrapper > .btn.authorize:not(.modal-btn)") !== null) {
            customizeLoginForm();
        }
    });
}});

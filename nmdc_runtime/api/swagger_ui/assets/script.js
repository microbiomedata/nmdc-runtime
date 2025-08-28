console.debug("Listening for event: nmdcInit");
window.addEventListener("nmdcInit", (event) => {{
    console.debug("Detected event: nmdcInit");

    // Get the DOM elements we'll be referencing below. 
    const tokenMaskTogglerEl = document.getElementById("token-mask-toggler");
    const tokenEl = document.getElementById("token");
    const tokenCopierEl = document.getElementById("token-copier");
    const tokenCopierMessageEl = document.getElementById("token-copier-message");
    const bodyEl = document.querySelector("body");
    
    // If all the token visibility-related elements are present (according to the logic implemented in `main.py`,
    // they will be present if an access token is available), set up the token visibility toggler and token copier.
    if (tokenMaskTogglerEl !== null && tokenEl !== null && tokenCopierEl !== null && tokenCopierMessageEl !== null) {
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

    // Customize the headers in the modal login form so they are more user-friendly.
    //
    // Note: We attach this event listener to the `body` element because that's the lowest-level
    //       element where we found that mounting it doesn't cause it to run too early (i.e. doesn't
    //       cause it to run _before_ the event handlers that mount the modal login form to the DOM
    //       have run). Our event handler needs that form to be mounted so it can access its elements.
    //       
    //       If we were to attach it to a lower-level element (e.g. directly to the "Authorize" button),
    //       we would have to, for example, make its body a `setTimeout(fn, 0)` callback in order to
    //       defer its execution until all the event handlers for the "click" even have run.
    //
    console.debug("Setting up event listener for customizing login form headers");
    bodyEl.addEventListener("click", (event) => {
        // Check whether the clicked element was the "Authorize" button or any of its descendants.
        if (event.target.closest(".auth-wrapper > .btn.authorize:not(.modal-btn)") !== null) {
            console.debug("Customizing login form headers");
            const modalContentEl = document.querySelector('.auth-wrapper .modal-ux-content');
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
        }
    });
}});

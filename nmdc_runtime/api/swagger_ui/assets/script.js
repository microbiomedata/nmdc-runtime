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
    // Note: We attach this event listener to a high-level DOM element instead of directly to
    //       the specific button that opens the login form (i.e. the "Authorize" button) so
    //       that our event handler doesn't run until the event handlers directly attached
    //       to that button have finished running. Those event handlers are responsible
    //       for creating and mounting the login form to the DOM, which is a prerequisite
    //       of us being able to access and modify its elements below.
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
                switch (el.textContent) {
                    // Note: This default string has a trailing space.
                    case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, password) ":
                        el.textContent = "User login";
                        break;
                    // Note: This default string has a trailing space.
                    case "OAuth2PasswordOrClientCredentialsBearer (OAuth2, clientCredentials) ":
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
